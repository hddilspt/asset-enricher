import os
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

from lxml import etree
from shapely.geometry import Point, Polygon
from shapely.strtree import STRtree

KML_NS = {"kml": "http://www.opengis.net/kml/2.2"}
PLACEMARK_TAG = "{http://www.opengis.net/kml/2.2}Placemark"


def normalize_sector(value: str) -> str:
    """
    Normalize sector strings from CSV into your canonical values:
      Retail | Office | Industrial & Logistics

    Adjust this mapping to match your real CSV values.
    """
    s = (value or "").strip().lower()

    if s in {"hsr", "retail"} or "retail" in s:
        return "Retail"
    if s in {"office"} or "office" in s:
        return "Office"
    if "industrial" in s or "logistics" in s:
        return "Industrial & Logistics"

    return (value or "").strip()


def infer_sector_from_filename(filename: str) -> Optional[str]:
    fn = (filename or "").lower()
    if "freguesia" in fn:
        return None

    if "hsr" in fn or "retail" in fn:
        return "Retail"
    if "office" in fn:
        return "Office"
    if "industrial" in fn or "logistics" in fn:
        return "Industrial & Logistics"

    return None


def _parse_kml_coordinates(text: str) -> List[Tuple[float, float]]:
    coords: List[Tuple[float, float]] = []
    for token in re.split(r"\s+", (text or "").strip()):
        if not token:
            continue
        parts = token.split(",")
        if len(parts) < 2:
            continue
        lon = float(parts[0])
        lat = float(parts[1])
        coords.append((lon, lat))
    return coords


def _polygons_from_placemark(pm: etree._Element) -> List[Polygon]:
    polys: List[Polygon] = []
    for poly_el in pm.findall(".//kml:Polygon", namespaces=KML_NS):
        coords_el = poly_el.find(
            ".//kml:outerBoundaryIs/kml:LinearRing/kml:coordinates",
            namespaces=KML_NS,
        )
        if coords_el is None or not (coords_el.text or "").strip():
            continue

        coords = _parse_kml_coordinates(coords_el.text)
        if len(coords) < 3:
            continue

        if coords[0] != coords[-1]:
            coords.append(coords[0])

        poly = Polygon(coords)
        if poly.is_empty or not poly.is_valid:
            continue

        polys.append(poly)

    return polys


@dataclass
class SpatialIndex:
    """
    - values is aligned to the geometry order used to build STRtree (same order as tree.geometries in Shapely 2).
    - geom_id_to_value supports Shapely 1, where STRtree.query returns geometry objects.
    """
    tree: STRtree
    values: List[str]
    geom_id_to_value: Dict[int, str]


def build_zone_indexes_from_paths(zone_kml_paths: List[str]) -> Dict[str, SpatialIndex]:
    by_sector_geoms: Dict[str, List[Polygon]] = {}
    by_sector_vals: Dict[str, List[str]] = {}

    for path in zone_kml_paths:
        filename = os.path.basename(path)
        sector = infer_sector_from_filename(filename)
        if not sector:
            continue

        doc = etree.parse(path)
        placemarks = doc.findall(".//kml:Placemark", namespaces=KML_NS)

        for pm in placemarks:
            name_el = pm.find("kml:name", namespaces=KML_NS)
            zone_name = (name_el.text or "").strip() if name_el is not None else ""
            if not zone_name:
                continue

            zone_full = f"{sector} - {zone_name}"
            polys = _polygons_from_placemark(pm)
            if not polys:
                continue

            by_sector_geoms.setdefault(sector, []).extend(polys)
            by_sector_vals.setdefault(sector, []).extend([zone_full] * len(polys))

    indexes: Dict[str, SpatialIndex] = {}
    for sector, geoms in by_sector_geoms.items():
        vals = by_sector_vals[sector]
        tree = STRtree(geoms)

        # For Shapely 1: query returns geometry objects -> we use id(geom) lookup
        geom_id_to_value = {id(g): v for g, v in zip(geoms, vals)}

        indexes[sector] = SpatialIndex(tree=tree, values=vals, geom_id_to_value=geom_id_to_value)

    return indexes


def build_freguesia_index_from_path(freguesias_kml_path: str) -> SpatialIndex:
    geoms: List[Polygon] = []
    vals: List[str] = []

    context = etree.iterparse(
        freguesias_kml_path,
        events=("end",),
        tag=PLACEMARK_TAG,
        huge_tree=True,
    )

    for _, pm in context:
        freg_name = None
        for sd in pm.findall(".//kml:SimpleData", namespaces=KML_NS):
            if sd.get("name") == "Freguesia":
                freg_name = (sd.text or "").strip()
                break

        if freg_name:
            polys = _polygons_from_placemark(pm)
            for poly in polys:
                geoms.append(poly)
                vals.append(freg_name)

        # free memory
        pm.clear()
        while pm.getprevious() is not None:
            del pm.getparent()[0]

    tree = STRtree(geoms)
    geom_id_to_value = {id(g): v for g, v in zip(geoms, vals)}
    return SpatialIndex(tree=tree, values=vals, geom_id_to_value=geom_id_to_value)


def lookup_point(index: SpatialIndex, lat: float, lon: float) -> Optional[str]:
    """
    Works with:
    - Shapely 2.x: STRtree.query returns numpy integer indices (or an Nx2 array for array-like inputs)
    - Shapely 1.x: STRtree.query returns geometry objects
    """
    pt = Point(float(lon), float(lat))

    candidates = index.tree.query(pt)
    if candidates is None:
        return None

    # Shapely 2 can return an (N,2) array of index pairs for array-like queries.
    # For a single geometry, we might still see just indices, but we handle both.
    try:
        import numpy as np  # Shapely 2 depends on numpy
        if isinstance(candidates, np.ndarray) and candidates.ndim == 2 and candidates.shape[1] == 2:
            tree_indices = candidates[:, 1]
        else:
            tree_indices = candidates
    except Exception:
        tree_indices = candidates

    # Case A: indices (Shapely 2)
    for item in tree_indices:
        # item can be numpy.int64, int, etc.
        try:
            i = int(item)
        except Exception:
            i = None

        if i is not None:
            # Shapely 2 provides tree.geometries aligned with indices :contentReference[oaicite:1]{index=1}
            geom = getattr(index.tree, "geometries", None)
            if geom is None:
                break  # can't resolve indices -> fallback below

            g = geom[i]
            if g.covers(pt):
                return index.values[i] if i < len(index.values) else None

    # Case B: geometries (Shapely 1)
    for geom in candidates:
        if hasattr(geom, "covers") and geom.covers(pt):
            return index.geom_id_to_value.get(id(geom))

    return None
