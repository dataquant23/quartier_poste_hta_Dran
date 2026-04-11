from __future__ import annotations

import os
import sqlite3
import tempfile
import threading
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from zipfile import BadZipFile

import geopandas as gpd
import pandas as pd
import requests
from django.conf import settings
from shapely import wkt
from django.db import connection

CALC_CRS = settings.CALC_CRS
MAP_CRS = settings.MAP_CRS
DEFAULT_RADIUS = settings.DEFAULT_RADIUS

os.environ["SHAPE_RESTORE_SHX"] = "YES"

_GROUP_OVERRIDE_TABLE = "quartier_group_precision_overrides"
_GROUP_DB_LOCK = threading.Lock()


@dataclass
class ResultPayload:
    table: pd.DataFrame
    postes_geojson: dict
    zones_geojson: dict
    pois_geojson: dict
    pharmacies_geojson: dict
    rayon: int


def _clean_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_token(value: str) -> str:
    txt = _clean_text(value)
    if not txt:
        return ""
    return " ".join(txt.split()).strip()


def _normalize_join_key(value: str) -> str:
    txt = _normalize_token(value)
    if not txt:
        return ""
    return txt.replace(" ", "").lower()


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


def _normalize_selected_keys(selected_keys: list[str]) -> list[str]:
    return [str(x).strip() for x in selected_keys if str(x).strip()]


def _build_group_key(selected_key: str, quartier_source: str) -> str:
    return f"{_normalize_token(selected_key)}||{_normalize_token(quartier_source)}"


def _get_sqlite_db_path() -> Path:
    db_name = settings.DATABASES["default"]["NAME"]
    return Path(db_name)


def _ensure_group_override_table() -> None:
    db_path = _get_sqlite_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with _GROUP_DB_LOCK:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_GROUP_OVERRIDE_TABLE} (
                    group_key TEXT PRIMARY KEY,
                    selected_key TEXT NOT NULL,
                    quartier_source TEXT NOT NULL,
                    precision_override TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.commit()


def _load_group_override_map() -> dict[str, str]:
    _ensure_group_override_table()
    db_path = _get_sqlite_db_path()

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT group_key, precision_override
            FROM {_GROUP_OVERRIDE_TABLE}
            """
        ).fetchall()

    out = {}
    for group_key, precision in rows:
        gk = _clean_text(group_key)
        pv = _clean_text(precision)
        # Important: un override vide doit aussi être pris en compte.
        if gk:
            out[gk] = pv
    return out


def save_group_precision_override(
    group_key: str,
    selected_key: str,
    quartier_source: str,
    precision_override: str,
) -> dict:
    group_key = _clean_text(group_key)
    selected_key = _clean_text(selected_key)
    quartier_source = _clean_text(quartier_source)
    precision_override = _clean_text(precision_override)

    if not group_key:
        group_key = _build_group_key(selected_key, quartier_source)

    if not group_key:
        raise ValueError("group_key manquant.")

    _ensure_group_override_table()
    db_path = _get_sqlite_db_path()

    with _GROUP_DB_LOCK:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                f"""
                INSERT INTO {_GROUP_OVERRIDE_TABLE}
                    (group_key, selected_key, quartier_source, precision_override, created_at, updated_at)
                VALUES
                    (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT(group_key) DO UPDATE SET
                    selected_key = excluded.selected_key,
                    quartier_source = excluded.quartier_source,
                    precision_override = excluded.precision_override,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (group_key, selected_key, quartier_source, precision_override),
            )
            conn.commit()

    return {
        "group_key": group_key,
        "selected_key": selected_key,
        "quartier_source": quartier_source,
        "precision_override": precision_override,
    }


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

'''
@lru_cache(maxsize=1)
def load_postes() -> gpd.GeoDataFrame:
    # On récupère les postes et on transforme la géométrie 4326 vers votre CALC_CRS (UTM 30N)
    query = f"""
        SELECT 
            code_poste as libelle_ref, 
            nom_poste as Nom_poste_ref,
            id_depart_hta,
            ST_AsText(ST_Transform(geom, {settings.CALC_CRS})) as wkt_geom
        FROM sig.poste_distribution
    """
    df = pd.read_sql(query, connection)
    df['geometry'] = df['wkt_geom'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs=settings.CALC_CRS)
    
    # Maintien de la clé technique pour ne pas casser le reste de l'app
    gdf["selected_key"] = gdf["libelle_ref"].astype(str) + "||" + gdf["Nom_poste_ref"].astype(str)
    return gdf
'''

@lru_cache(maxsize=1)
def load_postes() -> gpd.GeoDataFrame:
    df = pd.read_excel(settings.POSTES_XLS)
    df.columns = df.columns.str.strip()

    col_x = _first_existing_column(df, ["X", "x", "COORDX", "CoordX"])
    col_y = _first_existing_column(df, ["Y", "y", "COORDY", "CoordY"])
    if col_x is None or col_y is None:
        raise ValueError("Colonnes X/Y introuvables dans le fichier poste.")

    df[col_x] = pd.to_numeric(
        df[col_x].astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )
    df[col_y] = pd.to_numeric(
        df[col_y].astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )

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

    try:
        df = pd.read_excel(path)
    except BadZipFile as e:
        raise ValueError(
            f"Le fichier '{path.name}' est corrompu ou incomplet. Relance d'abord un rafraîchissement."
        ) from e

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


def _top_nearest_features(
    geom,
    gdf: gpd.GeoDataFrame,
    name_col: str,
    top_n: int = 6,
) -> list[tuple[str | None, object | None, float]]:
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
        join_key = _normalize_join_key(lib)
        if join_key and precision:
            out[join_key] = precision
    return out


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


def _split_precision_items(value: str) -> list[str]:
    txt = _normalize_token(value)
    if not txt:
        return []

    parts = [p.strip() for p in txt.split(",") if p.strip()]
    return [_normalize_token(p) for p in parts if _normalize_token(p)]


def _dedupe_tokens(values: list[str]) -> list[str]:
    out = []
    seen = set()

    for value in values:
        token = _normalize_token(value)
        key = token.lower()
        if token and key not in seen:
            seen.add(key)
            out.append(token)

    return out


def _concat_precision(poi_proche, pharmacie) -> str:
    parts = []
    for value in [poi_proche, pharmacie]:
        txt = _clean_text(value)
        if txt:
            parts.append(txt)
    return ", ".join(parts)


def _resolve_row_precision(row: pd.Series) -> str:
    precision = _clean_text(row.get("precision"))
    if precision:
        return precision

    precision_override = _clean_text(row.get("precision_override"))
    if precision_override:
        return precision_override

    precision_calculee = _clean_text(row.get("precision_calculee"))
    if precision_calculee:
        return precision_calculee

    poi_val = _clean_text(row.get("poi_proche"))
    pharma_val = _clean_text(row.get("pharmacie"))

    fallback_parts = []
    if poi_val:
        fallback_parts.append(poi_val)
    if pharma_val:
        fallback_parts.append(pharma_val)

    return ", ".join(fallback_parts)


def _build_group_precision_from_rows(grp: pd.DataFrame) -> str:
    all_tokens = []

    for _, row in grp.iterrows():
        precision_val = _resolve_row_precision(row)
        tokens = _split_precision_items(precision_val)
        all_tokens.extend(tokens)

    final_parts = _dedupe_tokens(all_tokens)
    return ", ".join(final_parts)


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

    result_cols = [
        c for c in ["libelle_ref", "Nom_poste_ref", "DR", "EXPLOITATION", "selected_key"]
        if c in subset.columns
    ]
    subset = subset[result_cols].drop_duplicates(subset=["selected_key"]).head(limit).copy()
    subset = subset.rename(columns={"libelle_ref": "libelle", "Nom_poste_ref": "Nom_poste"})
    return subset.to_dict(orient="records")

'''
def _build_final_dataset(rayon: int) -> gpd.GeoDataFrame:
    # On charge uniquement le fichier d'exception Excel (s'il est toujours pertinent métierement)
    poi_precision_map = _load_poi_propose_map()
    all_rows = []

    zone_query = """
    WITH cte_postes AS (
    SELECT 
        p.id_poste_distribution, -- IMPORTANT : Ajout de l'ID pour la jointure
        p.code_poste as libelle_ref,
        p.nom_poste as Nom_poste_ref,
        p.type_poste as "TYPE",
        d.nom_depart as "DEPART",
        e.nom_exploit as "EXPLOITATION",
        dr.nom_dr as "DR",
        ST_Transform(p.geom, 32630) as geom_32630
    FROM sig.poste_distribution p
    LEFT JOIN sig.depart_hta d ON p.id_depart_hta = d.id_depart
    LEFT JOIN sig.exploitation e ON p.id_exploit = e.id_exploit
    LEFT JOIN sig.direction_regionale dr ON e.id_dr = dr.id_dr
),
cte_quartiers AS (
    SELECT 
        q.id_quartier,           -- IMPORTANT : Ajout de l'ID pour la jointure
        q.nom_quartier, 
        c.nom_commune, 
        ST_Transform(q.geom, 32630) as geom_32630
    FROM sig.admin_quartier q
    JOIN sig.admin_commune c ON q.id_commune = c.id_commune
)
SELECT 
    p.*, 
    q.id_quartier,
    q.nom_quartier as quartier_source,
    q.nom_commune as commune_source,
    ovr.precision_override,      -- LA MAGIE EST ICI : On récupère la modification utilisateur directement
    ST_AsText(ST_Intersection(q.geom_32630, ST_Buffer(p.geom_32630, %(rayon)s))) as zone_wkt,
    ST_Distance(p.geom_32630, ST_Intersection(q.geom_32630, ST_Buffer(p.geom_32630, %(rayon)s))) as distance_poste_zone
FROM cte_postes p
JOIN cte_quartiers q ON ST_DWithin(p.geom_32630, q.geom_32630, %(rayon)s)
-- Jointure avec la nouvelle table normée
LEFT JOIN sig.app_precision_overrides ovr 
       ON ovr.id_poste_distribution = p.id_poste_distribution 
      AND ovr.id_quartier = q.id_quartier
WHERE ST_Area(ST_Intersection(q.geom_32630, ST_Buffer(p.geom_32630, %(rayon)s))) >= %(min_area)s
"""
    # df_zones contient maintenant l'override de l'utilisateur grâce au LEFT JOIN
    df_zones = pd.read_sql(
        zone_query, # La requête définie ci-dessus
        connection, 
        params={'rayon': rayon, 'min_area': settings.MIN_ZONE_AREA_M2}
    )

    if df_zones.empty:
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs=settings.CALC_CRS)

    # Requêtes OSM (identiques)
    query_landuse = "SELECT fclass FROM sig.osm_landuse WHERE ST_Intersects(geom, ST_Transform(ST_GeomFromText(%s, 32630), 4326)) LIMIT 1"
    query_pharmacie = "SELECT \"Nom\", ST_AsText(ST_Transform(geom, 32630)) as geom_wkt FROM sig.osm_pharmacies WHERE ST_Within(geom, ST_Transform(ST_GeomFromText(%s, 32630), 4326)) ORDER BY ST_Distance(ST_Transform(geom, 32630), ST_GeomFromText(%s, 32630)) ASC LIMIT 1"
    query_pois = "SELECT name, ST_AsText(ST_Transform(geom, 32630)) as geom_wkt, ST_Distance(ST_Transform(geom, 32630), ST_GeomFromText(%s, 32630)) as dist FROM sig.osm_pois WHERE ST_Within(geom, ST_Transform(ST_GeomFromText(%s, 32630), 4326)) AND name IS NOT NULL AND name != '' AND lower(name) != 'none' ORDER BY ST_Distance(ST_Transform(geom, 32630), ST_GeomFromText(%s, 32630)) ASC LIMIT 6"

    with connection.cursor() as cursor:
        for _, zone_row in df_zones.iterrows():
            zone_wkt = zone_row['zone_wkt']
            distance_poste_zone = round(zone_row['distance_poste_zone'], 2)
            piece = wkt.loads(zone_wkt)
            piece_centroid_wkt = piece.centroid.wkt

            cursor.execute(query_landuse, [zone_wkt])
            land_res = cursor.fetchone()
            zone_type = land_res[0] if land_res else "landuse"

            cursor.execute(query_pharmacie, [zone_wkt, piece_centroid_wkt])
            pharma_res = cursor.fetchone()
            pharmacie_name = _clean_text(pharma_res[0]) if pharma_res else None
            pharmacie_geom = wkt.loads(pharma_res[1]) if pharma_res else None

            poi_propose_val = poi_precision_map.get(zone_row["libelle_ref"])
            poi_candidates = []
            
            if poi_propose_val:
                poi_candidates = [(poi_propose_val, None, distance_poste_zone)]
            else:
                cursor.execute(query_pois, [piece_centroid_wkt, zone_wkt, piece_centroid_wkt])
                for p_name, p_geom_wkt, p_dist in cursor.fetchall():
                    poi_candidates.append((_clean_text(p_name), wkt.loads(p_geom_wkt), p_dist))

            if not poi_candidates:
                poi_candidates = [(None, None, distance_poste_zone)]

            for poi_name, poi_geom, poi_dist in poi_candidates:
                # Calcul de la précision automatique
                precision_calculee = _concat_precision(poi_name, pharmacie_name)
                
                # Récupération de l'override depuis la base (ou chaîne vide si NULL)
                precision_override = _clean_text(zone_row.get("precision_override"))
                
                # Règle métier : l'override utilisateur écrase le calcul automatique
                precision_finale = precision_override if precision_override else precision_calculee

                # Construction de la ligne
                row = {
                    "id_poste_distribution": zone_row["id_poste_distribution"], # On passe l'ID réel au front-end
                    "id_quartier": zone_row["id_quartier"],                     # On passe l'ID réel au front-end
                    "libelle": zone_row["libelle_ref"],
                    "Nom_poste": zone_row["Nom_poste_ref"],
                    "selected_key": f"{zone_row['libelle_ref']}||{zone_row['Nom_poste_ref']}", # Maintenu pour la rétrocompatibilité d'affichage
                    "DR": _clean_text(zone_row.get("DR")),
                    "EXPLOITATION": _clean_text(zone_row.get("EXPLOITATION")),
                    "DEPART": _clean_text(zone_row.get("DEPART")),
                    "TYPE": _clean_text(zone_row.get("TYPE")),
                    "quartier": _clean_text(zone_row["commune_source"]),
                    "quartier_source": _clean_text(zone_row["quartier_source"]),
                    "poi_proche": poi_name,
                    "POI_propose": poi_propose_val or None,
                    "pharmacie": pharmacie_name,
                    "precision_calculee": precision_calculee,
                    "precision_override": precision_override,
                    "precision": precision_finale, # La valeur finale prête à être affichée
                    "type_zone": zone_type,
                    "rayon_m": rayon,
                    "distance_poste_m": round(float(poi_dist), 2) if poi_name else distance_poste_zone
                }

                quartier_part = row["quartier_source"] or "QUARTIER"
                poi_part = _clean_text(poi_name) or _clean_text(pharmacie_name) or "VIDE"
                
                # Maintenu pour la génération de classe CSS/HTML dans les templates Django
                row["row_key"] = f"{row['selected_key']}||{quartier_part}||{poi_part}"
                row["group_key"] = f"{row['selected_key']}||{quartier_part}"

                row["geometry_zone_interet"] = _geom_to_wkt(piece)
                row["geometry_poi_proche"] = _geom_to_wkt(poi_geom)
                row["geometry_pharmacie_proche"] = _geom_to_wkt(pharmacie_geom)
                row["geometry"] = piece

                all_rows.append(row)

    if not all_rows:
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs=settings.CALC_CRS)

    return gpd.GeoDataFrame(all_rows, geometry="geometry", crs=settings.CALC_CRS)
'''

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

    for current in [quartiers, gdf_land, gdf_pois, gdf_roads, gdf_pharma]:
        try:
            _ = current.sindex
        except Exception:
            pass

    poste_cols = [
        c for c in postes.columns
        if c not in {"geometry", "QUARTIER", "geom_poste", "selected_key", "libelle_ref", "Nom_poste_ref"}
    ]

    all_rows = []

    for _, poste in postes.iterrows():
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

        poste_join_key = _normalize_join_key(poste["libelle_ref"])
        poi_propose_val = _clean_text(poi_precision_map.get(poste_join_key))

        for _, q_row in q_sel.iterrows():
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
                row["precision"] = ""
                row["type_zone"] = zone_type
                row["rayon_m"] = rayon
                row["distance_poste_m"] = round(float(poi_dist), 2) if poi_name else distance_poste_zone

                quartier_part = _clean_text(q_row.get("quartier_source")) or "QUARTIER"
                poi_part = _clean_text(poi_name) or _clean_text(pharmacie_name) or "VIDE"
                row["row_key"] = f"{poste['selected_key']}||{quartier_part}||{poi_part}"
                row["group_key"] = _build_group_key(poste["selected_key"], quartier_part)

                row["geometry_zone_interet"] = _geom_to_wkt(zone_interet)
                row["geometry_poi_proche"] = _geom_to_wkt(poi_geom)
                row["geometry_pharmacie_proche"] = _geom_to_wkt(pharmacie_geom)
                row["geometry"] = piece

                all_rows.append(row)

    if not all_rows:
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs=CALC_CRS)

    return gpd.GeoDataFrame(all_rows, geometry="geometry", crs=CALC_CRS)

def _load_overrides() -> pd.DataFrame:
    path = _ensure_overrides_file()
    try:
        df = pd.read_excel(path, dtype={"row_key": str, "precision_override": str})
    except BadZipFile:
        return pd.DataFrame(columns=["row_key", "precision_override"])

    df.columns = [str(c).strip() for c in df.columns]

    if "row_key" not in df.columns:
        df["row_key"] = ""
    if "precision_override" not in df.columns:
        df["precision_override"] = ""

    df["row_key"] = df["row_key"].fillna("").astype(str).apply(_clean_text)
    df["precision_override"] = df["precision_override"].fillna("").astype(str).apply(_clean_text)

    df = df[df["row_key"] != ""].drop_duplicates(subset=["row_key"], keep="last").copy()
    return df


def _apply_legacy_precision_overrides(df: pd.DataFrame) -> pd.DataFrame:
    overrides = _load_overrides()
    if df.empty:
        return df

    work = df.copy()

    if overrides.empty:
        if "precision_override" not in work.columns:
            work["precision_override"] = ""
        if "precision_calculee" not in work.columns:
            work["precision_calculee"] = ""
        work["precision_override"] = work["precision_override"].fillna("").apply(_clean_text)
        work["precision_calculee"] = work["precision_calculee"].fillna("").apply(_clean_text)
        work["precision"] = work.apply(
            lambda row: row["precision_override"] if row["precision_override"] else row["precision_calculee"],
            axis=1,
        )
        return work

    merged = work.merge(overrides, on="row_key", how="left", suffixes=("", "_ovr"))

    if "precision_override_ovr" in merged.columns:
        merged["precision_override"] = merged["precision_override_ovr"].fillna(merged.get("precision_override"))
        merged = merged.drop(columns=["precision_override_ovr"])

    merged["precision_override"] = merged["precision_override"].fillna("").apply(_clean_text)
    merged["precision_calculee"] = merged.get("precision_calculee", "").fillna("").apply(_clean_text)
    merged["precision"] = merged.apply(
        lambda row: row["precision_override"] if row["precision_override"] else row["precision_calculee"],
        axis=1,
    )
    return merged


def _atomic_write_excel(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=path.suffix, dir=path.parent)
    tmp_path = Path(tmp.name)
    tmp.close()

    try:
        df.to_excel(tmp_path, index=False)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


def _atomic_write_geojson(gdf: gpd.GeoDataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=path.suffix, dir=path.parent)
    tmp_path = Path(tmp.name)
    tmp.close()

    try:
        gdf.to_file(tmp_path, driver="GeoJSON")
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


def refresh_final_dataset(rayon: int = DEFAULT_RADIUS) -> pd.DataFrame:
    gdf = _build_final_dataset(rayon)

    if gdf.empty:
        _atomic_write_excel(pd.DataFrame(), Path(settings.PRECALC_XLSX))
        empty_gdf = gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs=CALC_CRS)
        _atomic_write_geojson(empty_gdf.to_crs(MAP_CRS), Path(settings.FINAL_GEOJSON))
        clear_runtime_caches()
        return pd.DataFrame()

    excel_df = gdf.drop(columns="geometry").copy()
    excel_df["geometry"] = gdf.geometry.apply(_geom_to_wkt)

    _atomic_write_excel(excel_df, Path(settings.PRECALC_XLSX))

    export_gdf = gdf.copy().to_crs(MAP_CRS)
    _atomic_write_geojson(export_gdf, Path(settings.FINAL_GEOJSON))

    clear_runtime_caches()
    return excel_df


def _ensure_final_dataset(rayon: int = DEFAULT_RADIUS) -> pd.DataFrame:
    df = load_precalc().copy()
    if df.empty:
        return df

    if "group_key" not in df.columns:
        df["group_key"] = df.apply(
            lambda row: _build_group_key(row.get("selected_key", ""), row.get("quartier_source", "")),
            axis=1,
        )

    return _apply_legacy_precision_overrides(df)


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
    keep_cols = [c for c in ["group_key", "row_key", "libelle", "Nom_poste", "quartier_source", "precision"] if c in gdf.columns]
    gdf = gdf[keep_cols + ["geometry"]].copy()

    return gdf.to_crs(MAP_CRS).__geo_interface__


def _build_pois_geojson(filtered: pd.DataFrame) -> dict:
    if filtered.empty or "geometry_poi_proche" not in filtered.columns:
        return {"type": "FeatureCollection", "features": []}

    df = filtered[["group_key", "row_key", "poi_proche", "geometry_poi_proche"]].copy()
    df["geometry"] = df["geometry_poi_proche"].apply(_safe_wkt)
    df = df[df["geometry"].notna()].copy()
    if df.empty:
        return {"type": "FeatureCollection", "features": []}

    gdf = gpd.GeoDataFrame(df[["group_key", "row_key", "poi_proche", "geometry"]], geometry="geometry", crs=CALC_CRS)
    gdf = gdf.drop_duplicates(subset=["poi_proche", "geometry"])

    return gdf.to_crs(MAP_CRS).__geo_interface__


def _build_pharmacies_geojson(filtered: pd.DataFrame) -> dict:
    if filtered.empty or "geometry_pharmacie_proche" not in filtered.columns:
        return {"type": "FeatureCollection", "features": []}

    df = filtered[["group_key", "row_key", "pharmacie", "geometry_pharmacie_proche"]].copy()
    df["geometry"] = df["geometry_pharmacie_proche"].apply(_safe_wkt)
    df = df[df["geometry"].notna()].copy()
    if df.empty:
        return {"type": "FeatureCollection", "features": []}

    gdf = gpd.GeoDataFrame(df[["group_key", "row_key", "pharmacie", "geometry"]], geometry="geometry", crs=CALC_CRS)
    gdf = gdf.drop_duplicates(subset=["pharmacie", "geometry"])

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
        "military": 90,
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


def _aggregate_priority_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "group_key",
                "row_key",
                "libelle",
                "Nom_poste",
                "quartier_source",
                "quartier_label",
                "precision",
                "details",
                "row_keys",
                "selected_key",
            ]
        )

    work = df.copy()
    for col in ["selected_key", "libelle", "Nom_poste", "quartier_source", "precision", "row_key", "group_key"]:
        if col not in work.columns:
            work[col] = ""

    work["selected_key"] = work["selected_key"].apply(_normalize_token)
    work["libelle"] = work["libelle"].apply(_normalize_token)
    work["Nom_poste"] = work["Nom_poste"].apply(_normalize_token)
    work["quartier_source"] = work["quartier_source"].apply(_normalize_token)
    work["precision"] = work["precision"].apply(_clean_text)
    work["row_key"] = work["row_key"].apply(_clean_text)
    work["group_key"] = work.apply(
        lambda row: _clean_text(row.get("group_key")) or _build_group_key(row.get("selected_key", ""), row.get("quartier_source", "")),
        axis=1,
    )

    group_override_map = _load_group_override_map()
    grouped_rows = []

    for (selected_key, quartier_source), grp in work.groupby(
        ["selected_key", "quartier_source"],
        dropna=False,
        sort=False,
    ):
        grp = grp.copy()

        if "distance_poste_m" in grp.columns:
            grp["distance_poste_m"] = pd.to_numeric(grp["distance_poste_m"], errors="coerce").fillna(999999)
            grp = grp.sort_values("distance_poste_m", ascending=True)

        first = grp.iloc[0]
        details = []
        row_keys = []
        seen_detail_keys = set()

        poi_names = _unique_texts(grp.get("poi_proche", pd.Series(dtype=str)).tolist())
        pharmacie_names = _unique_texts(grp.get("pharmacie", pd.Series(dtype=str)).tolist())

        for _, row in grp.iterrows():
            row_key = _clean_text(row.get("row_key"))
            poi_value = _clean_text(row.get("poi_proche"))
            pharmacie_value = _clean_text(row.get("pharmacie"))
            detail_precision = _resolve_row_precision(row)

            if row_key and row_key not in row_keys:
                row_keys.append(row_key)

            detail_key = (
                _normalize_token(detail_precision).lower(),
                _normalize_token(poi_value).lower(),
                _normalize_token(pharmacie_value).lower(),
            )
            if detail_key in seen_detail_keys:
                continue
            seen_detail_keys.add(detail_key)

            details.append(
                {
                    "row_key": row_key,
                    "precision": detail_precision,
                    "poi_proche": poi_value,
                    "pharmacie": pharmacie_value,
                }
            )

        group_key = _build_group_key(selected_key, quartier_source)
        has_group_override = group_key in group_override_map
        group_override = _clean_text(group_override_map.get(group_key, ""))
        precision_calculee_groupe = _build_group_precision_from_rows(grp)

        # Si l'utilisateur a explicitement vidé la précision, on garde ce vide.
        if has_group_override:
            precision_finale = group_override
        else:
            precision_finale = precision_calculee_groupe

        grouped_rows.append(
            {
                "group_key": group_key,
                "row_key": _clean_text(first.get("row_key")),
                "selected_key": _clean_text(selected_key),
                "libelle": _clean_text(first.get("libelle")),
                "Nom_poste": _clean_text(first.get("Nom_poste")),
                "quartier_source": _clean_text(quartier_source),
                "quartier_label": _clean_text(quartier_source),
                "precision": precision_finale,
                "precision_override": group_override,
                "precision_calculee": precision_calculee_groupe,
                "poi_names": poi_names,
                "pharmacie_names": pharmacie_names,
                "details": details,
                "row_keys": row_keys,
            }
        )

    out = pd.DataFrame(grouped_rows)
    sort_cols = [c for c in ["libelle", "Nom_poste", "quartier_source"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols).reset_index(drop=True)
    return out


def build_table_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "group_key",
                "row_key",
                "libelle",
                "Nom_poste",
                "quartier_source",
                "precision",
                "details",
                "row_keys",
                "selected_key",
            ]
        )

    filtered_for_table = _build_priority_table(df, max_per_quartier=3, top_distance_pool=6)
    grouped = _aggregate_priority_table(filtered_for_table)

    display_cols = [
        c
        for c in [
            "group_key",
            "row_key",
            "libelle",
            "Nom_poste",
            "quartier_source",
            "quartier_label",
            "precision",
            "precision_override",
            "precision_calculee",
            "poi_names",
            "pharmacie_names",
            "details",
            "row_keys",
            "selected_key",
        ]
        if c in grouped.columns
    ]
    return grouped[display_cols].fillna("").reset_index(drop=True)


def compute_payload(selected_keys: list[str], rayon: int = DEFAULT_RADIUS) -> ResultPayload:
    selected_keys = _normalize_selected_keys(selected_keys)
    df = _ensure_final_dataset(rayon)

    empty_geojson = {"type": "FeatureCollection", "features": []}
    empty_table = pd.DataFrame(columns=["group_key", "row_key", "libelle", "Nom_poste", "quartier_source", "precision"])

    if df.empty or not selected_keys:
        return ResultPayload(empty_table, empty_geojson, empty_geojson, empty_geojson, empty_geojson, rayon)

    if "selected_key" not in df.columns:
        return ResultPayload(empty_table, empty_geojson, empty_geojson, empty_geojson, empty_geojson, rayon)

    filtered = df[df["selected_key"].astype(str).isin(selected_keys)].copy()
    if filtered.empty:
        return ResultPayload(empty_table, empty_geojson, empty_geojson, empty_geojson, empty_geojson, rayon)

    table = build_table_rows(filtered)

    return ResultPayload(
        table=table,
        postes_geojson=_build_postes_geojson(filtered),
        zones_geojson=_build_zones_geojson(filtered),
        pois_geojson=_build_pois_geojson(filtered),
        pharmacies_geojson=_build_pharmacies_geojson(filtered),
        rayon=rayon,
    )



def _apply_download_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    """
    Règles appliquées UNIQUEMENT au fichier téléchargé.
    """
    if df.empty:
        return df.copy()

    work = df.copy()

    for col in ["selected_key", "quartier_source", "POI_propose", "precision_calculee", "precision_override", "precision"]:
        if col not in work.columns:
            work[col] = ""

    work["selected_key"] = work["selected_key"].fillna("").astype(str).apply(_clean_text)
    work["quartier_source"] = work["quartier_source"].fillna("").astype(str).apply(_clean_text)
    work["POI_propose"] = work["POI_propose"].fillna("").astype(str).apply(_clean_text)
    work["precision_calculee"] = work["precision_calculee"].fillna("").astype(str).apply(_clean_text)
    work["precision_override"] = work["precision_override"].fillna("").astype(str).apply(_clean_text)
    work["precision"] = work["precision"].fillna("").astype(str).apply(_clean_text)

    mask_poi = work["POI_propose"] != ""
    work.loc[mask_poi, "precision_calculee"] = work.loc[mask_poi, "POI_propose"]

    if "pharmacie" in work.columns:
        work.loc[mask_poi, "pharmacie"] = ""

    no_override = work["precision_override"] == ""
    work.loc[no_override, "precision"] = work.loc[no_override, "precision_calculee"]
    work.loc[~no_override, "precision"] = work.loc[~no_override, "precision_override"]

    keep_idx = []
    for selected_key, grp in work.groupby("selected_key", dropna=False, sort=False):
        poi_values = [v for v in grp["POI_propose"].tolist() if _clean_text(v)]
        if not poi_values:
            keep_idx.extend(grp.index.tolist())
            continue

        poi_key = _normalize_join_key(poi_values[0])
        quartier_keys = grp["quartier_source"].apply(_normalize_join_key)
        matches = grp[quartier_keys == poi_key]

        if not matches.empty:
            keep_idx.extend(matches.index.tolist())
        else:
            keep_idx.extend(grp.index.tolist())

    work = work.loc[sorted(set(keep_idx))].copy()
    return work


def _apply_download_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    """
    Règles appliquées UNIQUEMENT au fichier téléchargé.

    Famille 1 - Règles POI_propose.xlsx
    -----------------------------------
    - jointure robuste déjà assurée en amont via _normalize_join_key
    - si POI_propose existe et qu'il n'y a pas d'override utilisateur pour le groupe,
      la précision d'export devient POI_propose seul
    - dans ce cas on n'ajoute pas la pharmacie
    - comme cette précision peut être répétée sur tous les quartiers du poste :
        * si POI_propose correspond à un quartier_source, on garde seulement cette ligne
        * sinon, on garde un seul quartier de façon stable

    Famille 2 - Règles base SQLite
    ------------------------------
    - si un override SQL existe, on respecte sa valeur
    - même si cette valeur est vide
    - cela doit écraser la précision calculée
    """
    if df.empty:
        return df.copy()

    work = df.copy()

    for col in [
        "selected_key",
        "group_key",
        "row_key",
        "quartier_source",
        "POI_propose",
        "precision_calculee",
        "precision_override",
        "precision",
    ]:
        if col not in work.columns:
            work[col] = ""

    work["selected_key"] = work["selected_key"].fillna("").astype(str).apply(_clean_text)
    work["group_key"] = work["group_key"].fillna("").astype(str).apply(_clean_text)
    work["row_key"] = work["row_key"].fillna("").astype(str).apply(_clean_text)
    work["quartier_source"] = work["quartier_source"].fillna("").astype(str).apply(_clean_text)
    work["POI_propose"] = work["POI_propose"].fillna("").astype(str).apply(_clean_text)
    work["precision_calculee"] = work["precision_calculee"].fillna("").astype(str).apply(_clean_text)
    work["precision_override"] = work["precision_override"].fillna("").astype(str).apply(_clean_text)
    work["precision"] = work["precision"].fillna("").astype(str).apply(_clean_text)

    # Relecture SQL fiable, y compris overrides vides
    group_override_map = _load_group_override_map()

    # Etape A: base de précision d'export
    # Si POI_propose existe, la précision calculée d'export devient POI_propose seul.
    # Cette règle ne s'applique que s'il n'existe pas d'override utilisateur.
    mask_poi = work["POI_propose"] != ""
    work.loc[mask_poi, "precision_calculee"] = work.loc[mask_poi, "POI_propose"]

    # Quand la précision vient du POI proposé, on n'ajoute pas la pharmacie.
    if "pharmacie" in work.columns:
        work.loc[mask_poi, "pharmacie"] = ""

    # Etape B: la valeur finale respecte toujours la base SQL si override présent
    final_precisions = []
    for _, row in work.iterrows():
        group_key = _clean_text(row.get("group_key"))
        if group_key in group_override_map:
            final_precisions.append(_clean_text(group_override_map[group_key]))
        else:
            final_precisions.append(_clean_text(row.get("precision_calculee")))
    work["precision"] = final_precisions

    # Etape C: réduction des quartiers quand POI_propose a répété la même précision
    kept_groups = []

    for selected_key, grp in work.groupby("selected_key", dropna=False, sort=False):
        grp = grp.copy()

        poi_values = [v for v in grp["POI_propose"].tolist() if _clean_text(v)]
        if not poi_values:
            kept_groups.append(grp)
            continue

        poi_key = _normalize_join_key(poi_values[0])
        grp["quartier_key"] = grp["quartier_source"].apply(_normalize_join_key)

        matches = grp[grp["quartier_key"] == poi_key].copy()

        if not matches.empty:
            kept = matches
        else:
            # Si aucun quartier ne correspond au POI proposé, on conserve un seul quartier
            # de façon stable et reproductible.
            sort_cols = ["quartier_source"]
            if "row_key" in grp.columns:
                sort_cols.append("row_key")
            kept = grp.sort_values(sort_cols, kind="stable").head(1).copy()

        kept_groups.append(kept.drop(columns=["quartier_key"], errors="ignore"))

    if not kept_groups:
        return work.iloc[0:0].copy()

    return pd.concat(kept_groups, ignore_index=True)

def export_priority_dataset_to_excel(temp_path: Path, rayon: int = DEFAULT_RADIUS) -> Path:
    temp_path.parent.mkdir(parents=True, exist_ok=True)

    df = _ensure_final_dataset(rayon)
    if df.empty:
        pd.DataFrame().to_excel(temp_path, index=False)
        return temp_path

    # Appliquer les règles POI_propose + SQL uniquement pour le fichier téléchargé
    export_source_df = _apply_download_business_rules(df)

    grouped = build_table_rows(export_source_df)

    if grouped.empty:
        pd.DataFrame().to_excel(temp_path, index=False)
        return temp_path

    info_cols = [
        c for c in [
            "selected_key",
            "DR",
            "EXPLOITATION",
            "DEPART",
            "TYPE",
        ]
        if c in export_source_df.columns
    ]

    info_df = export_source_df[info_cols].drop_duplicates(subset=["selected_key"]).copy()

    export_df = grouped.merge(
        info_df,
        on="selected_key",
        how="left",
    )

    ordered_cols = [
        "DR",
        "EXPLOITATION",
        "DEPART",
        "TYPE",
        "libelle",
        "Nom_poste",
        "quartier_source",
        "precision",
    ]
    ordered_cols = [c for c in ordered_cols if c in export_df.columns]

    export_df = export_df[ordered_cols].fillna("").copy()

    # Règle finale d'export :
    # s'il ne reste qu'une seule précision non vide pour le poste,
    # on ne garde que cette ligne.
    group_cols = [c for c in ["libelle", "Nom_poste"] if c in export_df.columns]

    if group_cols:
        cleaned_groups = []

        for _, group in export_df.groupby(group_cols, dropna=False):
            group = group.copy()

            precision_non_vide = group["precision"].astype(str).str.strip().ne("")
            nb_precisions_non_vides = precision_non_vide.sum()

            if nb_precisions_non_vides == 1:
                cleaned_groups.append(group.loc[precision_non_vide])
            else:
                cleaned_groups.append(group)

        if cleaned_groups:
            export_df = pd.concat(cleaned_groups, ignore_index=True)
        else:
            export_df = export_df.iloc[0:0].copy()

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

def load_precalc_raw() -> pd.DataFrame:

    path = Path(settings.PRECALC_XLSX)
    if not path.exists():
        return pd.DataFrame()

    try:
        df = pd.read_excel(path)
    except Exception:
        return pd.DataFrame()

    df.columns = [str(c).strip() for c in df.columns]
    return df


def _get_user_group_override_df() -> pd.DataFrame:
    _ensure_group_override_table()
    db_path = _get_sqlite_db_path()

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT group_key, selected_key, quartier_source, precision_override
            FROM {_GROUP_OVERRIDE_TABLE}
            """
        ).fetchall()

    if not rows:
        return pd.DataFrame(columns=["group_key", "selected_key", "quartier_source", "precision_override"])

    df = pd.DataFrame(
        rows,
        columns=["group_key", "selected_key", "quartier_source", "precision_override"]
    )

    for col in df.columns:
        df[col] = df[col].fillna("").astype(str).apply(_clean_text)

    df = df[df["precision_override"] != ""].copy()
    return df

def compute_bilan_stats() -> dict:
   
    df = load_precalc_raw()

    if df.empty or "selected_key" not in df.columns:
        return {
            "nb_postes_avec_precision_source": 0,
            "nb_postes_sans_precision_source": 0,
            "nb_postes_precision_existante_modifiee": 0,
            "nb_postes_sans_precision_completee_par_user": 0,
            "nb_total_postes": 0,
        }

    work = df.copy()

    for col in ["selected_key", "precision_calculee", "quartier_source", "row_key"]:
        if col not in work.columns:
            work[col] = ""

    work["selected_key"] = work["selected_key"].fillna("").astype(str).apply(_clean_text)
    work["precision_calculee"] = work["precision_calculee"].fillna("").astype(str).apply(_clean_text)
    work["quartier_source"] = work["quartier_source"].fillna("").astype(str).apply(_clean_text)
    work["row_key"] = work["row_key"].fillna("").astype(str).apply(_clean_text)

    work = work[work["selected_key"] != ""].copy()

    if work.empty:
        return {
            "nb_postes_avec_precision_source": 0,
            "nb_postes_sans_precision_source": 0,
            "nb_postes_precision_existante_modifiee": 0,
            "nb_postes_sans_precision_completee_par_user": 0,
            "nb_total_postes": 0,
        }

    # 1) Présence de précision dans la source brute
    source_by_poste = (
        work.groupby("selected_key")["precision_calculee"]
        .apply(lambda s: any(_clean_text(v) for v in s.tolist()))
        .reset_index(name="has_source_precision")
    )

    # 2) Nouveaux overrides SQLite
    sqlite_override_df = _get_user_group_override_df()
    sqlite_postes = set()

    if not sqlite_override_df.empty and "selected_key" in sqlite_override_df.columns:
        sqlite_postes = set(
            sqlite_override_df["selected_key"]
            .dropna()
            .astype(str)
            .apply(_clean_text)
            .tolist()
        )
        sqlite_postes = {x for x in sqlite_postes if x}

    # 3) Anciens overrides Excel
    legacy_override_df = _get_legacy_user_override_df()
    legacy_postes = set()

    if not legacy_override_df.empty:
        # relier row_key -> selected_key via la source brute
        map_df = work[["selected_key", "row_key"]].drop_duplicates().copy()
        merged_legacy = map_df.merge(
            legacy_override_df,
            on="row_key",
            how="inner",
        )

        if not merged_legacy.empty:
            legacy_postes = set(
                merged_legacy["selected_key"]
                .dropna()
                .astype(str)
                .apply(_clean_text)
                .tolist()
            )
            legacy_postes = {x for x in legacy_postes if x}

    # 4) Union de toutes les modifications utilisateur
    all_override_postes = sqlite_postes | legacy_postes

    source_by_poste["has_user_override"] = source_by_poste["selected_key"].isin(all_override_postes)

    nb_total_postes = int(len(source_by_poste))
    nb_postes_avec_precision_source = int(source_by_poste["has_source_precision"].sum())
    nb_postes_sans_precision_source = int((~source_by_poste["has_source_precision"]).sum())

    nb_postes_precision_existante_modifiee = int(
        (
            (source_by_poste["has_source_precision"] == True)
            & (source_by_poste["has_user_override"] == True)
        ).sum()
    )

    nb_postes_sans_precision_completee_par_user = int(
        (
            (source_by_poste["has_source_precision"] == False)
            & (source_by_poste["has_user_override"] == True)
        ).sum()
    )

    return {
        "nb_postes_avec_precision_source": nb_postes_avec_precision_source,
        "nb_postes_sans_precision_source": nb_postes_sans_precision_source,
        "nb_postes_precision_existante_modifiee": nb_postes_precision_existante_modifiee,
        "nb_postes_sans_precision_completee_par_user": nb_postes_sans_precision_completee_par_user,
        "nb_total_postes": nb_total_postes,
    }

def _get_legacy_user_override_df() -> pd.DataFrame:
    """
    Lit les anciens overrides stockés dans precision_overrides.xlsx
    et ne garde que les vraies modifications non vides.
    """
    df = _load_overrides().copy()
    if df.empty:
        return pd.DataFrame(columns=["row_key", "precision_override"])

    if "row_key" not in df.columns:
        df["row_key"] = ""
    if "precision_override" not in df.columns:
        df["precision_override"] = ""

    df["row_key"] = df["row_key"].fillna("").astype(str).apply(_clean_text)
    df["precision_override"] = df["precision_override"].fillna("").astype(str).apply(_clean_text)

    df = df[(df["row_key"] != "") & (df["precision_override"] != "")].copy()
    return df

def export_bilan_to_excel(temp_path: Path) -> Path:
    stats = compute_bilan_stats()

    df = pd.DataFrame(
        [
        {
            "Indicateur": "Postes avec précision",
            "Valeur": stats["nb_postes_avec_precision_source"],
        },
        {
            "Indicateur": "Postes sans précision",
            "Valeur": stats["nb_postes_sans_precision_source"],
        },
        {
            "Indicateur": "Postes modifiés",
            "Valeur": stats["nb_postes_precision_existante_modifiee"],
        },
        {
            "Indicateur": "Postes complétés",
            "Valeur": stats["nb_postes_sans_precision_completee_par_user"],
        },
        {
            "Indicateur": "Total postes",
            "Valeur": stats["nb_total_postes"],
        },
        ]
    )

    temp_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(temp_path, index=False)
    return temp_path