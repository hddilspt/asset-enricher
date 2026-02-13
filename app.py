import gzip
import io
import os
import shutil
import tempfile
from typing import Dict, List, Optional

import pandas as pd
from flask import Flask, jsonify, request, send_file

from kml_index import (
    SpatialIndex,
    build_freguesia_index_from_path,
    build_zone_indexes_from_paths,
    lookup_point,
    normalize_sector,
)

app = Flask(__name__)

# ---- Config ----
API_KEY = os.environ.get("API_KEY", "")  # set this in Railway for security

DATA_DIR = os.environ.get("DATA_DIR", "data")
ZONES_DIR = os.environ.get("ZONES_DIR", os.path.join(DATA_DIR, "zones"))

# We are using the gzipped file in the repo:
FREGUESIAS_GZ_PATH = os.environ.get(
    "FREGUESIAS_GZ_PATH", os.path.join(DATA_DIR, "Freguesias.kml.gz")
)

# We'll unzip to /tmp (works on Railway)
FREGUESIAS_UNZIPPED_PATH = os.environ.get(
    "FREGUESIAS_UNZIPPED_PATH", os.path.join(tempfile.gettempdir(), "Freguesias.kml")
)

# ---- Caches (loaded once per process; we run 1 worker) ----
ZONE_INDEXES: Dict[str, SpatialIndex] = {}
FREG_INDEX: Optional[SpatialIndex] = None


def require_api_key():
    if not API_KEY:
        return None  # allow unauth in dev
    if request.headers.get("X-API-Key") != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401
    return None


def ensure_freguesias_unzipped(gz_path: str, out_path: str) -> str:
    """
    Ensure the gzipped KML is uncompressed to a real KML file.
    Reuses the unzipped file if it already exists and looks non-trivial.
    """
    if not os.path.exists(gz_path):
        raise FileNotFoundError(f"Missing gzipped freguesias file at: {gz_path}")

    # reuse if already unzipped
    if os.path.exists(out_path) and os.path.getsize(out_path) > 10_000_000:
        return out_path

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with gzip.open(gz_path, "rb") as f_in, open(out_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

    return out_path


def load_indexes():
    """
    Load zone indexes and freguesia index at startup.
    Called at module import time so gunicorn --preload builds once.
    """
    global ZONE_INDEXES, FREG_INDEX

    # ---- Zones ----
    zone_paths: List[str] = []
    if os.path.isdir(ZONES_DIR):
        for fn in os.listdir(ZONES_DIR):
            if not fn.lower().endswith(".kml"):
                continue
            if "freguesia" in fn.lower():
                continue
            zone_paths.append(os.path.join(ZONES_DIR, fn))

    ZONE_INDEXES = build_zone_indexes_from_paths(zone_paths)

    # ---- Freguesias ----
    kml_path = ensure_freguesias_unzipped(FREGUESIAS_GZ_PATH, FREGUESIAS_UNZIPPED_PATH)
    FREG_INDEX = build_freguesia_index_from_path(kml_path)


# Load indexes immediately (important for preload)
load_indexes()


@app.get("/health")
def health():
    return {
        "ok": True,
        "zones_loaded": sorted(list(ZONE_INDEXES.keys())),
        "freguesia_loaded": FREG_INDEX is not None,
    }


@app.post("/enrich")
def enrich():
    auth = require_api_key()
    if auth:
        return auth

    if "assets" not in request.files:
        return jsonify({"error": "Missing form file field 'assets'"}), 400

    if FREG_INDEX is None:
        return jsonify({"error": "Freguesia index not loaded"}), 500

    output_format = (request.form.get("output_format") or "csv").lower().strip()
    if output_format not in ("csv", "xlsx"):
        return jsonify({"error": "output_format must be csv or xlsx"}), 400

    # Allow overriding column names from Power Automate later
    asset_name_col = request.form.get("asset_name_col", "[Asset Name]")
    lat_col = request.form.get("lat_col", "[Lat]")
    lon_col = request.form.get("lon_col", "[Long]")
    sector_col = request.form.get("sector_col", "[Sector]")

    f = request.files["assets"]

    try:
        df = pd.read_csv(f)
    except Exception as e:
        return jsonify({"error": f"Failed to read CSV: {str(e)}"}), 400

    for col in [asset_name_col, lat_col, lon_col, sector_col]:
        if col not in df.columns:
            return jsonify({"error": f"Missing column '{col}' in assets file"}), 400

    zones_out: List[Optional[str]] = []
    freg_out: List[Optional[str]] = []

    # Iterate rows
    for _, row in df.iterrows():
        raw_sector = str(row[sector_col])
        sector = normalize_sector(raw_sector)

        # Coordinates
        try:
            lat = float(row[lat_col])
            lon = float(row[lon_col])
        except Exception:
            zones_out.append(None)
            freg_out.append(None)
            continue

        # Zone: only for matching sector index
        zone_val = None
        sector_index = ZONE_INDEXES.get(sector)
        if sector_index is not None:
            zone_val = lookup_point(sector_index, lat=lat, lon=lon)
        zones_out.append(zone_val)

        # Freguesia: always attempt
        freg = lookup_point(FREG_INDEX, lat=lat, lon=lon)
        freg_out.append(freg)

    out = pd.DataFrame(
        {
            "Asset Name": df[asset_name_col],
            "Lat": df[lat_col],
            "Long": df[lon_col],
            "Sector": df[sector_col],
            "Zone": zones_out,        # already "Sector - ZoneName"
            "Freguesia": freg_out,
        }
    )

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

    # xlsx
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

