from __future__ import annotations

import json
from pathlib import Path

from django.conf import settings
from django.http import FileResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_POST

from .services import (
    build_table_rows,
    compute_payload,
    export_priority_dataset_to_excel,
    get_poste_context,
    load_precalc,
    refresh_final_dataset,
    save_precision_override,
    search_postes,
)


@ensure_csrf_cookie
def index(request):
    precalc = load_precalc()
    precalc_rows = []

    if not precalc.empty:
        precalc_rows = build_table_rows(precalc).head(8).to_dict(orient="records")

    return render(
        request,
        "quartier/index.html",
        {
            "DEFAULT_RADIUS": settings.DEFAULT_RADIUS,
            "precalc_rows": precalc_rows,
        },
    )


@require_GET
def api_search_postes(request):
    q = request.GET.get("q", "")
    results = search_postes(q)
    return JsonResponse({"ok": True, "results": results})


@require_GET
def api_compute(request):
    selected = request.GET.getlist("selected")
    try:
        rayon = max(1, int(request.GET.get("rayon", settings.DEFAULT_RADIUS)))
    except Exception:
        rayon = settings.DEFAULT_RADIUS

    payload = compute_payload(selected, rayon)

    return JsonResponse(
        {
            "ok": True,
            "rayon": payload.rayon,
            "rows": payload.table.fillna("").to_dict(orient="records"),
            "postes_geojson": payload.postes_geojson,
            "zones_geojson": payload.zones_geojson,
            "pois_geojson": payload.pois_geojson,
        }
    )


@require_GET
def api_refresh(request):
    try:
        rayon = max(1, int(request.GET.get("rayon", settings.DEFAULT_RADIUS)))
    except Exception:
        rayon = settings.DEFAULT_RADIUS

    df = refresh_final_dataset(rayon)
    return JsonResponse({"ok": True, "rows": len(df), "rayon": rayon})


@require_GET
def api_poste_context(request):
    selected_key = request.GET.get("selected_key", "")
    return JsonResponse({"ok": True, "data": get_poste_context(selected_key)})


@require_POST
def api_update_precision(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
        row_key = payload.get("row_key", "")
        precision_override = payload.get("precision", "")

        print("DEBUG update_precision payload =", payload)
        print("DEBUG row_key =", row_key)
        print("DEBUG precision_override =", precision_override)

        row = save_precision_override(row_key, precision_override)

        print("DEBUG update_precision OK")
        return JsonResponse({"ok": True, "row": row})

    except Exception as e:
        print("DEBUG update_precision ERROR =", repr(e))
        return JsonResponse({"ok": False, "error": str(e)}, status=400)


@require_GET
def download_excel(request):
    try:
        rayon = int(request.GET.get("rayon", settings.DEFAULT_RADIUS))
    except Exception:
        rayon = settings.DEFAULT_RADIUS

    temp_path = Path(settings.DATA_DIR) / "download_quartiers_alimentes_par_poste.xlsx"
    export_priority_dataset_to_excel(temp_path, rayon)

    return FileResponse(
        open(temp_path, "rb"),
        as_attachment=True,
        filename="quartiers_alimentes_par_poste.xlsx",
    )