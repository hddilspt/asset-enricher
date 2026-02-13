import io
import os
from typing import Dict, List

import pandas as pd
from flask import Flask, request, send_file, jsonify

from kml_index import (
    SpatialIndex,
    build_freguesia_index_from_path,
    build_zone_indexes_from_paths,
    lookup_point,
)

app = Flask(__name__)

API_KEY = os.environ.get("API_KEY", "")
DATA_DIR = os.environ.get("DATA_DIR", "data")
FREGUESIAS_PATH = os.environ.get("FREGUESIAS_PATH", os.path.join(DATA_DIR, "Freguesias.kml"))
ZONES_DIR = os.environ.get("ZONES_DIR", os.path.join(DATA_DIR, "zones"))

# Cached indexes (loaded once per container)
ZONE_INDEXES: Dict[str, SpatialIndex] = {}
FREG_INDEX: SpatialIndex | None = None


def require_api_key():
    if not API_KEY:
        return  # allow if you forgot to set it (dev), but set it in prod
    if request.headers.get("X-API-Key") != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401


@app.before_first_request
def load_indexes():
    global ZONE_INDEXES, FREG_INDEX

    # Load zone KMLs from disk (recommended)
    zone_paths: List[str] = []
    if os.path.isdir(ZONES_DIR):
        for fn in os.listdir(ZONES_DIR):
            if fn.lower().endswith(".kml") and "freguesia" not in fn.lower():
                zone_paths.append(os.path.join(ZONES_DIR, fn))
    ZONE_INDEXES = build_zone_indexes_from_paths(zone_paths)

    # Load freguesias once (big file)
    if not os.path.exists(FREGUESIAS_PATH):
        raise FileNotFoundError(f"Freguesias KML not found at {FREGUESIAS_PATH}")
    FREG_INDEX = build_freguesia_index_from_path(FREGUESIAS_PATH)


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/enrich")
def enrich():
    auth = require_api_key()
    if auth:
        return auth

    if "assets" not in request.files:
        return jsonify({"error": "Missing form file 'assets'"}), 400

    output_format = (request.form.get("output_format") or "csv").lower().strip()
    if output_format not in ("csv", "xlsx"):
        return jsonify({"error": "output_format must be csv or xlsx"}), 400

    # Column mapping (defaults; you can override later via request.form if you want)
    asset_name_col = request.form.get("asset_name_col", "Asset Name")
    lat_col = request.form.get("lat_col", "Lat")
    lon_col = request.form.get("lon_col", "Long")
    sector_col = request.form.get("sector_col", "Sector")

    f = request.files["assets"]
    df = pd.read_csv(f)

    for col in [asset_name_col, lat_col, lon_col, sector_col]:
        if col not in df.columns:
            return jsonify({"error": f"Missing column '{col}' in assets file"}), 400

    if FREG_INDEX is None:
        return jsonify({"error": "Freguesia index not loaded"}), 500

    zones_out = []
    freg_out = []

    for _, row in df.iterrows():
        sector = str(row[sector_col]).strip()
        lat = float(row[lat_col])
        lon = float(row[lon_col])

        # Zone: only search within the correct sector index
        zone_val = None
        idx = ZONE_INDEXES.get(sector)
        if idx is not None:
            zone_val = lookup_point(idx, lat=lat, lon=lon)
        zones_out.append(zone_val)

        # Freguesia: always attempt
        freg = lookup_point(FREG_INDEX, lat=lat, lon=lon)
        freg_out.append(freg)

    out = pd.DataFrame({
        "Asset Name": df[asset_name_col],
        "Lat": df[lat_col],
        "Long": df[lon_col],
        "Sector": df[sector_col],
        "Zone": zones_out,
        "Freguesia": freg_out,
    })

    buf = io.BytesIO()
    if output_format == "csv":
        out.to_csv(buf, index=False)
        buf.seek(0)
        return send_file(
            buf,
            mimetype="text/csv",
            as_attachment=True,
            download_name="assets_enriched.csv",
        )
    else:
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            out.to_excel(writer, index=False, sheet_name="Assets")
        buf.seek(0)
        return send_file(
            buf,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name="assets_enriched.xlsx",
        )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)