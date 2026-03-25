from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from django.conf import settings
from shapely import wkt

CALC_CRS = settings.CALC_CRS
MAP_CRS = settings.MAP_CRS
DEFAULT_RADIUS = settings.DEFAULT_RADIUS

os.environ["SHAPE_RESTORE_SHX"] = "YES"


@dataclass
class ResultPayload:
    table: pd.DataFrame
    postes_geojson: dict
    zones_geojson: dict
    pois_geojson: dict
    rayon: int


def _clean_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _geom_to_wkt(geom):
    try:
        if geom is None or geom.is_empty:
            return None
        return geom.wkt
    except Exception:
        return None


def _safe_wkt(value):
    try:
        if pd.isna(value):
            return None
        txt = str(value).strip()
        if not txt:
            return None
        return wkt.loads(txt)
    except Exception:
        return None


def _first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _concat_precision(poi_proche, pharmacie) -> str:
    parts = []
    for value in [poi_proche, pharmacie]:
        txt = _clean_text(value)
        if txt:
            parts.append(txt)
    return ", ".join(parts)


def _normalize_selected_keys(selected_keys: list[str]) -> list[str]:
    return [str(x).strip() for x in selected_keys if str(x).strip()]


def _ensure_overrides_file() -> Path:
    path = Path(settings.PRECISION_OVERRIDES_XLSX)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        pd.DataFrame(columns=["row_key", "precision_override"]).to_excel(path, index=False)
    return path


def clear_runtime_caches() -> None:
    load_postes.cache_clear()
    load_quartiers.cache_clear()
    load_precalc.cache_clear()
    load_final_geojson.cache_clear()


@lru_cache(maxsize=1)
def load_postes() -> gpd.GeoDataFrame:
    df = pd.read_excel(settings.POSTES_XLS)
    df.columns = df.columns.str.strip()

    col_x = _first_existing_column(df, ["X", "x", "COORDX", "CoordX"])
    col_y = _first_existing_column(df, ["Y", "y", "COORDY", "CoordY"])
    if col_x is None or col_y is None:
        raise ValueError("Colonnes X/Y introuvables dans le fichier poste.")

    df[col_x] = pd.to_numeric(df[col_x].astype(str).str.replace(",", ".", regex=False), errors="coerce")
    df[col_y] = pd.to_numeric(df[col_y].astype(str).str.replace(",", ".", regex=False), errors="coerce")

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(_clean_text)

    df = df.dropna(subset=[col_x, col_y]).copy()

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[col_x], df[col_y]),
        crs=CALC_CRS,
    )

    lib_col = _first_existing_column(gdf, ["libelle", "Libellé", "Libelle", "LIBELLE"])
    nom_col = _first_existing_column(gdf, ["Nom_poste", "Nom poste", "NOM_POSTE"])
    if lib_col is None or nom_col is None:
        raise ValueError("Colonnes libelle/Nom_poste introuvables dans le fichier poste.")

    gdf["libelle_ref"] = gdf[lib_col].apply(_clean_text)
    gdf["Nom_poste_ref"] = gdf[nom_col].apply(_clean_text)
    gdf["selected_key"] = gdf["libelle_ref"] + "||" + gdf["Nom_poste_ref"]
    return gdf


@lru_cache(maxsize=1)
def load_quartiers() -> gpd.GeoDataFrame:
    df = pd.read_excel(settings.QUARTIER_XLSX)
    df.columns = df.columns.str.strip()

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(_clean_text)

    if "geometry" not in df.columns:
        raise ValueError("La colonne geometry est absente de quartier.xlsx")

    df["geometry"] = df["geometry"].apply(_safe_wkt)
    df = df[df["geometry"].notna()].copy()

    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=CALC_CRS)
    gdf["quartier_source"] = gdf["nom"].apply(_clean_text) if "nom" in gdf.columns else ""
    gdf["commune_source"] = gdf["commune"].apply(_clean_text) if "commune" in gdf.columns else ""
    return gdf


@lru_cache(maxsize=1)
def load_precalc() -> pd.DataFrame:
    path = Path(settings.PRECALC_XLSX)
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_excel(path)
    df.columns = [str(c).strip() for c in df.columns]
    return df


@lru_cache(maxsize=1)
def load_final_geojson() -> dict:
    path = Path(settings.FINAL_GEOJSON)
    if not path.exists():
        return {"type": "FeatureCollection", "features": []}
    gdf = gpd.read_file(path)
    return gdf.__geo_interface__


def _read_osm_shapefile(path: Path) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)
    gdf.columns = gdf.columns.str.strip()
    if gdf.crs is None:
        gdf = gdf.set_crs(settings.OSM_SOURCE_CRS, allow_override=True)
    return gdf.to_crs(CALC_CRS)


def _bbox_candidates(gdf: gpd.GeoDataFrame, geom) -> gpd.GeoDataFrame:
    if gdf.empty or geom is None or geom.is_empty:
        return gdf.iloc[0:0].copy()

    minx, miny, maxx, maxy = geom.bounds
    try:
        idx = list(gdf.sindex.intersection((minx, miny, maxx, maxy)))
        if not idx:
            return gdf.iloc[0:0].copy()
        return gdf.iloc[idx].copy()
    except Exception:
        return gdf.cx[minx:maxx, miny:maxy].copy()


def _nearest_feature_info(geom, gdf: gpd.GeoDataFrame, name_col: str) -> tuple[str | None, object | None]:
    if gdf.empty:
        return None, None
    distances = gdf.geometry.distance(geom.centroid)
    idx = distances.idxmin()
    name = gdf.loc[idx, name_col] if name_col in gdf.columns else None
    feat_geom = gdf.loc[idx, "geometry"]
    name = _clean_text(name)
    return (name or None), feat_geom


def _top_nearest_features(geom, gdf: gpd.GeoDataFrame, name_col: str, top_n: int = 6) -> list[tuple[str | None, object | None, float]]:
    if gdf.empty or geom is None or geom.is_empty:
        return []

    center = geom.centroid
    work = gdf.copy()
    work["__dist__"] = work.geometry.distance(center)
    work = work.sort_values("__dist__", ascending=True).head(top_n)

    out = []
    for _, row in work.iterrows():
        name = _clean_text(row.get(name_col))
        feat_geom = row.get("geometry")
        dist = float(row.get("__dist__", 999999))
        out.append((name or None, feat_geom, dist))
    return out


def _nearest_landuse_type(geom, land_gdf: gpd.GeoDataFrame) -> str | None:
    if land_gdf.empty:
        return None
    inter = land_gdf[land_gdf.intersects(geom)]
    if inter.empty:
        return None
    if "fclass" in inter.columns:
        vals = inter["fclass"].dropna().astype(str).str.strip()
        vals = vals[vals != ""]
        if len(vals) > 0:
            return vals.iloc[0]
    return "landuse"


def _load_pharmacies() -> gpd.GeoDataFrame:
    gdf = gpd.read_file(settings.PHARMACIES_GEOJSON)
    gdf.columns = gdf.columns.str.strip()
    if gdf.crs is None:
        gdf = gdf.set_crs(settings.OSM_SOURCE_CRS, allow_override=True)
    gdf = gdf.to_crs(CALC_CRS)
    if "Nom" not in gdf.columns:
        gdf["Nom"] = None
    gdf["Nom"] = gdf["Nom"].apply(_clean_text)
    return gdf[gdf.geometry.notna()].copy()


def _load_poi_propose_map() -> dict[str, str]:
    path = Path(settings.POI_PROPOSE_XLSX)
    if not path.exists():
        return {}

    df = pd.read_excel(path)
    df.columns = df.columns.str.strip()
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(_clean_text)

    col_lib = _first_existing_column(df, ["Libellé", "Libelle", "libelle", "LIBELLE"])
    col_precision = _first_existing_column(df, ["Précision", "Precision", "precision", "PRECISION"])
    if col_lib is None or col_precision is None:
        return {}

    out = {}
    for _, row in df.iterrows():
        lib = _clean_text(row.get(col_lib))
        precision = _clean_text(row.get(col_precision))
        if lib and precision:
            out[lib] = precision
    return out


def reverse_geocode(lat, lon):
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {"lat": lat, "lon": lon, "format": "json"}
    headers = {"User-Agent": "quartier-app"}

    try:
        r = requests.get(url, params=params, headers=headers, timeout=5)
        r.raise_for_status()
        data = r.json()
        addr = data.get("address", {})
        return {
            "rue": addr.get("road"),
            "quartier_geo": addr.get("suburb"),
            "voisinage": addr.get("neighbourhood"),
            "name": data.get("name") or addr.get("name"),
            "village": addr.get("village"),
            "city": addr.get("city") or addr.get("town") or addr.get("municipality"),
            "state": addr.get("state"),
        }
    except Exception:
        return {}


def search_postes(query: str = "", limit: int = 20) -> list[dict]:
    gdf = load_postes()
    q = (query or "").strip().lower()

    if q:
        mask = (
            gdf["Nom_poste_ref"].str.lower().str.contains(q, na=False)
            | gdf["libelle_ref"].str.lower().str.contains(q, na=False)
        )
        subset = gdf.loc[mask].copy()
    else:
        subset = gdf.copy()

    result_cols = [c for c in ["libelle_ref", "Nom_poste_ref", "DR", "EXPLOITATION", "selected_key"] if c in subset.columns]
    subset = subset[result_cols].drop_duplicates(subset=["selected_key"]).head(limit).copy()
    subset = subset.rename(columns={"libelle_ref": "libelle", "Nom_poste_ref": "Nom_poste"})
    return subset.to_dict(orient="records")


def _build_final_dataset(rayon: int) -> gpd.GeoDataFrame:
    postes = load_postes().copy()
    quartiers = load_quartiers().copy()
    poi_precision_map = _load_poi_propose_map()

    gdf_land = _read_osm_shapefile(settings.LANDUSE_SHP)
    gdf_pois = _read_osm_shapefile(settings.POIS_SHP)
    gdf_roads = _read_osm_shapefile(settings.ROADS_SHP)
    gdf_pharma = _load_pharmacies()

    if "name" not in gdf_pois.columns:
        gdf_pois["name"] = None
    gdf_pois["name"] = gdf_pois["name"].apply(_clean_text)
    gdf_pois = gdf_pois[
        (gdf_pois.geometry.notna())
        & (gdf_pois["name"] != "")
        & (gdf_pois["name"].str.lower() != "none")
    ].copy()

    if "fclass" not in gdf_land.columns:
        gdf_land["fclass"] = None
    gdf_land["fclass"] = gdf_land["fclass"].apply(_clean_text)
    gdf_land = gdf_land[gdf_land.geometry.notna()].copy()

    gdf_roads = gdf_roads[gdf_roads.geometry.notna()].copy()
    gdf_pharma = gdf_pharma[gdf_pharma.geometry.notna()].copy()
    quartiers = quartiers[quartiers.geometry.notna()].copy()
    postes = postes[postes.geometry.notna()].copy()

    quartier_keep = [c for c in ["quartier_source", "commune_source", "geometry"] if c in quartiers.columns]
    quartiers = quartiers[quartier_keep].copy()

    land_keep = [c for c in ["fclass", "geometry"] if c in gdf_land.columns]
    gdf_land = gdf_land[land_keep].copy()

    pois_keep = [c for c in ["name", "geometry"] if c in gdf_pois.columns]
    gdf_pois = gdf_pois[pois_keep].copy()

    pharma_keep = [c for c in ["Nom", "geometry"] if c in gdf_pharma.columns]
    gdf_pharma = gdf_pharma[pharma_keep].copy()

    try:
        _ = quartiers.sindex
    except Exception:
        pass
    try:
        _ = gdf_land.sindex
    except Exception:
        pass
    try:
        _ = gdf_pois.sindex
    except Exception:
        pass
    try:
        _ = gdf_roads.sindex
    except Exception:
        pass
    try:
        _ = gdf_pharma.sindex
    except Exception:
        pass

    poste_cols = [
        c for c in postes.columns
        if c not in {"geometry", "QUARTIER", "geom_poste", "selected_key", "libelle_ref", "Nom_poste_ref"}
    ]

    all_rows = []

    print("Début génération dataset final")
    print("Nb postes :", len(postes))
    print("Nb quartiers :", len(quartiers))

    for poste_idx, (_, poste) in enumerate(postes.iterrows(), start=1):
        if poste_idx % 100 == 0:
            print(f"Postes traités : {poste_idx}/{len(postes)}")

        point_poste = poste.geometry
        if point_poste is None or point_poste.is_empty:
            continue

        buffer_poste = point_poste.buffer(rayon)

        q_candidates = _bbox_candidates(quartiers, buffer_poste)
        if q_candidates.empty:
            continue

        q_sel = q_candidates[q_candidates.intersects(buffer_poste)].copy()
        if q_sel.empty:
            continue

        poi_propose_val = poi_precision_map.get(poste["libelle_ref"])

        for q_idx, q_row in q_sel.iterrows():
            quartier_geom = q_row.geometry
            if quartier_geom is None or quartier_geom.is_empty:
                continue

            zone_interet = quartier_geom.intersection(buffer_poste)
            if zone_interet.is_empty:
                continue

            piece = zone_interet
            if piece.is_empty or piece.area < settings.MIN_ZONE_AREA_M2:
                continue

            land_candidates = _bbox_candidates(gdf_land, zone_interet)
            pois_candidates = _bbox_candidates(gdf_pois, zone_interet)
            pharma_candidates = _bbox_candidates(gdf_pharma, zone_interet)
            _ = _bbox_candidates(gdf_roads, zone_interet)

            land_local = land_candidates[land_candidates.intersects(zone_interet)].copy() if not land_candidates.empty else land_candidates
            pois_local = pois_candidates[pois_candidates.within(zone_interet)].copy() if not pois_candidates.empty else pois_candidates
            pharma_local = pharma_candidates[pharma_candidates.within(zone_interet)].copy() if not pharma_candidates.empty else pharma_candidates

            zone_type = _nearest_landuse_type(piece, land_local)
            distance_poste_zone = round(point_poste.distance(piece), 2)
            pharmacie_name, pharmacie_geom = _nearest_feature_info(piece, pharma_local, "Nom")

            if poi_propose_val:
                poi_candidates = [(poi_propose_val, None, distance_poste_zone)]
            else:
                poi_candidates = _top_nearest_features(piece, pois_local, "name", top_n=6)

            if not poi_candidates:
                poi_candidates = [(None, None, distance_poste_zone)]

            for poi_name, poi_geom, poi_dist in poi_candidates:
                precision = _concat_precision(poi_name, pharmacie_name)

                row = {}
                for col in poste_cols:
                    row[col] = poste.get(col)

                row["libelle"] = poste["libelle_ref"]
                row["Nom_poste"] = poste["Nom_poste_ref"]
                row["selected_key"] = poste["selected_key"]
                row["quartier"] = _clean_text(q_row.get("commune_source"))
                row["quartier_source"] = _clean_text(q_row.get("quartier_source"))
                row["poi_proche"] = poi_name
                row["POI_propose"] = poi_propose_val or None
                row["pharmacie"] = pharmacie_name
                row["precision_calculee"] = precision
                row["precision_override"] = None
                row["precision"] = precision
                row["type_zone"] = zone_type
                row["rayon_m"] = rayon
                row["distance_poste_m"] = round(float(poi_dist), 2) if poi_name else distance_poste_zone

                poi_part = _clean_text(poi_name) if poi_name else "VIDE"
                row["row_key"] = f"{poste['selected_key']}||{q_idx}||{poi_part}"

                row["geometry_zone_interet"] = _geom_to_wkt(zone_interet)
                row["geometry_poi_proche"] = _geom_to_wkt(poi_geom)
                row["geometry_pharmacie_proche"] = _geom_to_wkt(pharmacie_geom)
                row["geometry"] = piece

                all_rows.append(row)

    print("Fin génération dataset final")
    print("Nb lignes générées :", len(all_rows))

    if not all_rows:
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs=CALC_CRS)

    return gpd.GeoDataFrame(all_rows, geometry="geometry", crs=CALC_CRS)


def _load_overrides() -> pd.DataFrame:
    path = _ensure_overrides_file()
    df = pd.read_excel(path, dtype={"row_key": str, "precision_override": str})
    df.columns = [str(c).strip() for c in df.columns]

    if "row_key" not in df.columns:
        df["row_key"] = ""
    if "precision_override" not in df.columns:
        df["precision_override"] = ""

    df["row_key"] = df["row_key"].fillna("").astype(str).apply(_clean_text)
    df["precision_override"] = df["precision_override"].fillna("").astype(str).apply(_clean_text)

    df = df[df["row_key"] != ""].drop_duplicates(subset=["row_key"], keep="last").copy()
    return df

def _apply_precision_overrides(df: pd.DataFrame) -> pd.DataFrame:
    overrides = _load_overrides()
    if df.empty:
        return df

    if overrides.empty:
        df["precision_override"] = df.get("precision_override", "").apply(_clean_text)
        df["precision_calculee"] = df.get("precision_calculee", "").apply(_clean_text)
        df["precision"] = df.apply(
            lambda row: row["precision_override"] if row["precision_override"] else row["precision_calculee"],
            axis=1,
        )
        return df

    merged = df.merge(overrides, on="row_key", how="left", suffixes=("", "_ovr"))
    if "precision_override_ovr" in merged.columns:
        merged["precision_override"] = merged["precision_override_ovr"].fillna(merged.get("precision_override"))
        merged = merged.drop(columns=["precision_override_ovr"])

    merged["precision_override"] = merged["precision_override"].fillna("").apply(_clean_text)
    merged["precision_calculee"] = merged["precision_calculee"].fillna("").apply(_clean_text)
    merged["precision"] = merged.apply(
        lambda row: row["precision_override"] if row["precision_override"] else row["precision_calculee"],
        axis=1,
    )
    return merged


def refresh_final_dataset(rayon: int = DEFAULT_RADIUS) -> pd.DataFrame:
    gdf = _build_final_dataset(rayon)

    if gdf.empty:
        pd.DataFrame().to_excel(settings.PRECALC_XLSX, index=False)
        empty_gdf = gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs=CALC_CRS)
        empty_gdf.to_crs(MAP_CRS).to_file(settings.FINAL_GEOJSON, driver="GeoJSON")
        clear_runtime_caches()
        return pd.DataFrame()

    gdf = _apply_precision_overrides(gdf)

    excel_df = gdf.drop(columns="geometry").copy()
    excel_df["geometry"] = gdf.geometry.apply(_geom_to_wkt)
    excel_df.to_excel(settings.PRECALC_XLSX, index=False)

    export_gdf = gdf.copy().to_crs(MAP_CRS)
    export_gdf.to_file(settings.FINAL_GEOJSON, driver="GeoJSON")

    clear_runtime_caches()
    return excel_df

def save_precision_override(row_key: str, precision_override: str) -> dict:
    row_key = _clean_text(row_key)
    precision_override = _clean_text(precision_override)

    if not row_key:
        raise ValueError("row_key manquant.")

    path = _ensure_overrides_file()

    df = pd.read_excel(path, dtype={"row_key": str, "precision_override": str})
    df.columns = [str(c).strip() for c in df.columns]

    if "row_key" not in df.columns:
        df["row_key"] = ""
    if "precision_override" not in df.columns:
        df["precision_override"] = ""

    df["row_key"] = df["row_key"].fillna("").astype(str).apply(_clean_text)
    df["precision_override"] = df["precision_override"].fillna("").astype(str).apply(_clean_text)

    if row_key in set(df["row_key"]):
        df.loc[df["row_key"] == row_key, "precision_override"] = str(precision_override)
    else:
        df = pd.concat(
            [df, pd.DataFrame([{"row_key": str(row_key), "precision_override": str(precision_override)}])],
            ignore_index=True,
        )

    df["row_key"] = df["row_key"].astype(str)
    df["precision_override"] = df["precision_override"].astype(str)

    df = df.drop_duplicates(subset=["row_key"], keep="last")
    df.to_excel(path, index=False)

    final_df = load_precalc().copy()
    if not final_df.empty and "row_key" in final_df.columns:
        final_df["row_key"] = final_df["row_key"].fillna("").astype(str).apply(_clean_text)

        if "precision_override" not in final_df.columns:
            final_df["precision_override"] = ""

        final_df["precision_override"] = final_df["precision_override"].fillna("").astype(str)
        final_df["precision_calculee"] = final_df.get("precision_calculee", "").fillna("").astype(str).apply(_clean_text)

        final_df.loc[final_df["row_key"] == row_key, "precision_override"] = str(precision_override)
        final_df["precision"] = final_df.apply(
            lambda row: row["precision_override"] if _clean_text(row["precision_override"]) else row["precision_calculee"],
            axis=1,
        )

        final_df.to_excel(settings.PRECALC_XLSX, index=False)
        clear_runtime_caches()

        row = final_df[final_df["row_key"] == row_key].head(1).fillna("").to_dict(orient="records")
        if row:
            return row[0]

    return {"row_key": row_key, "precision_override": precision_override}
def _ensure_final_dataset(rayon: int = DEFAULT_RADIUS) -> pd.DataFrame:
    return load_precalc().copy()


def _build_postes_geojson(filtered: pd.DataFrame) -> dict:
    if filtered.empty:
        return {"type": "FeatureCollection", "features": []}

    postes = load_postes().copy()
    keys = filtered["selected_key"].dropna().astype(str).unique().tolist()
    selected_postes = postes[postes["selected_key"].isin(keys)].copy()
    if selected_postes.empty:
        return {"type": "FeatureCollection", "features": []}

    gdf = gpd.GeoDataFrame(
        selected_postes[["selected_key", "libelle_ref", "Nom_poste_ref", "DR", "EXPLOITATION", "geometry"]].copy(),
        geometry="geometry",
        crs=CALC_CRS,
    ).rename(columns={"libelle_ref": "libelle", "Nom_poste_ref": "Nom_poste"})

    return gdf.to_crs(MAP_CRS).__geo_interface__


def _build_zones_geojson(filtered: pd.DataFrame) -> dict:
    if filtered.empty or "geometry" not in filtered.columns:
        return {"type": "FeatureCollection", "features": []}

    df = filtered.copy()
    df["geometry"] = df["geometry"].apply(_safe_wkt)
    df = df[df["geometry"].notna()].copy()
    if df.empty:
        return {"type": "FeatureCollection", "features": []}

    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=CALC_CRS)
    keep_cols = [c for c in ["row_key", "libelle", "Nom_poste", "quartier_source", "precision"] if c in gdf.columns]
    gdf = gdf[keep_cols + ["geometry"]].copy()

    return gdf.to_crs(MAP_CRS).__geo_interface__


def _build_pois_geojson(filtered: pd.DataFrame) -> dict:
    if filtered.empty or "geometry_poi_proche" not in filtered.columns:
        return {"type": "FeatureCollection", "features": []}

    df = filtered[["row_key", "poi_proche", "geometry_poi_proche"]].copy()
    df["geometry"] = df["geometry_poi_proche"].apply(_safe_wkt)
    df = df[df["geometry"].notna()].copy()
    if df.empty:
        return {"type": "FeatureCollection", "features": []}

    gdf = gpd.GeoDataFrame(df[["row_key", "poi_proche", "geometry"]], geometry="geometry", crs=CALC_CRS)
    gdf = gdf.drop_duplicates(subset=["poi_proche", "geometry"])

    return gdf.to_crs(MAP_CRS).__geo_interface__


def _normalize_zone_type(value: str) -> str:
    txt = _clean_text(value).lower()
    return txt if txt else "(vides)"


def _zone_type_priority(zone_type: str) -> int:
    zone_type = _normalize_zone_type(zone_type)
    priorities = {
        "(vides)": 100,
        "commercial": 90,
        "retail": 85,
        "park": 80,
        "recreation_ground": 80,
        "industrial": 70,
        "residential": 60,
        "cemetery": 50,
        "military": 50,
        "grass": 20,
        "meadow": 20,
        "forest": 15,
        "scrub": 15,
        "farmland": 10,
        "orchard": 10,
    }
    return priorities.get(zone_type, 30)


def _precision_quality_score(precision: str) -> int:
    txt = _clean_text(precision)
    if not txt:
        return 0

    score = 30
    if len(txt) >= 8:
        score += 10
    if len(txt) >= 15:
        score += 10

    generic_terms = {"pharmacie", "banque", "clinique", "commerce", "maison", "immeuble"}
    if txt.lower() in generic_terms:
        score -= 10

    return score


def _source_bonus(row: pd.Series) -> int:
    bonus = 0
    if _clean_text(row.get("POI_propose")):
        bonus += 50
    if _clean_text(row.get("poi_proche")):
        bonus += 20
    if _clean_text(row.get("pharmacie")):
        bonus += 10
    if _clean_text(row.get("precision")):
        bonus += 15
    return bonus


def _build_priority_table(df: pd.DataFrame, max_per_quartier: int = 3, top_distance_pool: int = 6) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    work = df.copy()

    for col in ["selected_key", "quartier_source", "precision", "type_zone"]:
        if col not in work.columns:
            work[col] = ""

    work["selected_key"] = work["selected_key"].apply(_clean_text)
    work["quartier_source"] = work["quartier_source"].apply(_clean_text)
    work["precision"] = work["precision"].apply(_clean_text)
    work["type_zone"] = work["type_zone"].apply(_normalize_zone_type)

    if "distance_poste_m" not in work.columns:
        work["distance_poste_m"] = 9999

    work["distance_poste_m"] = pd.to_numeric(work["distance_poste_m"], errors="coerce").fillna(9999)

    dedup_keys = ["selected_key", "quartier_source", "precision"]
    work = (
        work.sort_values("distance_poste_m", ascending=True)
        .drop_duplicates(subset=dedup_keys, keep="first")
        .copy()
    )

    work["distance_rank"] = (
        work.groupby(["selected_key", "quartier_source"])["distance_poste_m"]
        .rank(method="first", ascending=True)
    )
    work = work[work["distance_rank"] <= top_distance_pool].copy()

    work["distance_score"] = (1 / (work["distance_poste_m"] + 1)) * 1000
    work["zone_score"] = work["type_zone"].apply(_zone_type_priority)
    work["precision_score"] = work["precision"].apply(_precision_quality_score)
    work["source_score"] = work.apply(_source_bonus, axis=1)

    work["priority_score"] = (
        work["distance_score"] * 2
        + work["zone_score"]
        + work["precision_score"]
        + work["source_score"]
    )

    work.loc[work["precision"] == "", "priority_score"] -= 50

    work["final_rank"] = (
        work.groupby(["selected_key", "quartier_source"])["priority_score"]
        .rank(method="first", ascending=False)
    )
    work = work[work["final_rank"] <= max_per_quartier].copy()

    return work.drop(
        columns=[
            "distance_rank",
            "distance_score",
            "zone_score",
            "precision_score",
            "source_score",
            "priority_score",
            "final_rank",
        ],
        errors="ignore",
    )



def _unique_texts(values: list[str]) -> list[str]:
    out = []
    seen = set()
    for value in values:
        txt = _clean_text(value)
        key = txt.lower()
        if txt and key not in seen:
            seen.add(key)
            out.append(txt)
    return out


def _aggregate_priority_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "row_key",
            "libelle",
            "Nom_poste",
            "quartier_source",
            "quartier_label",
            "precision",
            "details",
            "row_keys",
        ])

    work = df.copy()
    for col in ["selected_key", "libelle", "Nom_poste", "quartier_source", "precision", "row_key"]:
        if col not in work.columns:
            work[col] = ""

    grouped_rows = []
    for (selected_key, quartier_source), grp in work.groupby(["selected_key", "quartier_source"], dropna=False, sort=False):
        grp = grp.copy()
        if "distance_poste_m" in grp.columns:
            grp["distance_poste_m"] = pd.to_numeric(grp["distance_poste_m"], errors="coerce").fillna(999999)
            grp = grp.sort_values("distance_poste_m", ascending=True)

        first = grp.iloc[0]
        details = []
        precision_parts = []
        row_keys = []

        for _, row in grp.iterrows():
            row_key = _clean_text(row.get("row_key"))
            precision_value = _clean_text(row.get("precision"))
            if precision_value:
                precision_parts.append(precision_value)
            if row_key:
                row_keys.append(row_key)
            details.append({
                "row_key": row_key,
                "precision": precision_value,
                "poi_proche": _clean_text(row.get("poi_proche")),
                "pharmacie": _clean_text(row.get("pharmacie")),
            })

        grouped_rows.append({
            "row_key": _clean_text(first.get("row_key")),
            "selected_key": _clean_text(selected_key),
            "libelle": _clean_text(first.get("libelle")),
            "Nom_poste": _clean_text(first.get("Nom_poste")),
            "quartier_source": _clean_text(quartier_source),
            "quartier_label": _clean_text(quartier_source),
            "precision": ", ".join(_unique_texts(precision_parts)),
            "details": details,
            "row_keys": row_keys,
        })

    out = pd.DataFrame(grouped_rows)
    sort_cols = [c for c in ["libelle", "Nom_poste", "quartier_source"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols).reset_index(drop=True)
    return out


def build_table_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["row_key", "libelle", "Nom_poste", "quartier_source", "precision", "details", "row_keys"])

    filtered_for_table = _build_priority_table(df, max_per_quartier=3, top_distance_pool=6)
    grouped = _aggregate_priority_table(filtered_for_table)

    display_cols = [
        c
        for c in [
            "row_key",
            "libelle",
            "Nom_poste",
            "quartier_source",
            "quartier_label",
            "precision",
            "details",
            "row_keys",
        ]
        if c in grouped.columns
    ]
    return grouped[display_cols].fillna("").reset_index(drop=True)


def compute_payload(selected_keys: list[str], rayon: int = DEFAULT_RADIUS) -> ResultPayload:
    selected_keys = _normalize_selected_keys(selected_keys)
    df = _ensure_final_dataset(rayon)

    empty_geojson = {"type": "FeatureCollection", "features": []}
    empty_table = pd.DataFrame(columns=["row_key", "libelle", "Nom_poste", "quartier_source", "precision"])

    if df.empty or not selected_keys:
        return ResultPayload(empty_table, empty_geojson, empty_geojson, empty_geojson, rayon)

    if "selected_key" not in df.columns:
        return ResultPayload(empty_table, empty_geojson, empty_geojson, empty_geojson, rayon)

    filtered = df[df["selected_key"].astype(str).isin(selected_keys)].copy()
    if filtered.empty:
        return ResultPayload(empty_table, empty_geojson, empty_geojson, empty_geojson, rayon)

    table = build_table_rows(filtered)

    return ResultPayload(
        table=table,
        postes_geojson=_build_postes_geojson(filtered),
        zones_geojson=_build_zones_geojson(filtered),
        pois_geojson=_build_pois_geojson(filtered),
        rayon=rayon,
    )


def export_priority_dataset_to_excel(temp_path: Path, rayon: int = DEFAULT_RADIUS) -> Path:
    temp_path.parent.mkdir(parents=True, exist_ok=True)

    df = _ensure_final_dataset(rayon)
    if df.empty:
        pd.DataFrame().to_excel(temp_path, index=False)
        return temp_path

    df_priority = _build_priority_table(df, max_per_quartier=3, top_distance_pool=6)

    base_cols = []
    postes = load_postes()
    poste_cols = [c for c in postes.columns if c not in {"geometry", "selected_key", "libelle_ref", "Nom_poste_ref"}]

    for col in poste_cols:
        if col in df_priority.columns and col not in base_cols:
            base_cols.append(col)

    extra_cols = [c for c in ["quartier_source", "precision"] if c in df_priority.columns]
    export_cols = [c for c in base_cols + extra_cols if c in df_priority.columns]

    export_df = df_priority[export_cols].copy()
    export_df.to_excel(temp_path, index=False)

    return temp_path


def get_poste_context(selected_key: str) -> dict:
    selected_key = _clean_text(selected_key)
    if not selected_key:
        return {}

    postes = load_postes().copy()
    row = postes[postes["selected_key"] == selected_key].head(1)
    if row.empty:
        return {}

    rec = row.iloc[0]
    point = rec.geometry
    gdf = gpd.GeoSeries([point], crs=CALC_CRS).to_crs(MAP_CRS)
    lon, lat = gdf.iloc[0].x, gdf.iloc[0].y

    return {
        "selected_key": selected_key,
        "libelle": rec["libelle_ref"],
        "Nom_poste": rec["Nom_poste_ref"],
        "DR": _clean_text(rec.get("DR")),
        "EXPLOITATION": _clean_text(rec.get("EXPLOITATION")),
        "lat": lat,
        "lon": lon,
        "geo_info": reverse_geocode(lat, lon),
    }