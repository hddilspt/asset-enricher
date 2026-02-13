import os
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

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

    # examples / aliases
    if s in {"hsr", "retail"} or "retail" in s:
        return "Retail"
    if s in {"office"} or "office" in s:
        return "Office"
    if "industrial" in s or "logistics" in s:
        return "Industrial & Logistics"

    # fallback: title-case it so keys are consistent-ish
    return (value or "").strip()


def infer_sector_from_filename(filename: str) -> Optional[str]:
    """
    Infer sector from the *zone* file name. Adjust as needed.
    """
    fn = (filename or "").lower()

    if "freguesia" in fn:
        return None

    # Your example logic:
    if "hsr" in fn or "retail" in fn:
        return "Retail"
    if "office" in fn:
        return "Office"
    if "industrial" in fn or "logistics" in fn:
        return "Industrial & Logistics"

    return None


def _parse_kml_coordinates(text: str) -> List[Tuple[float, float]]:
    """
    KML coordinates are "lon,lat[,alt]" separated by whitespace.
    Returns list of (lon, lat)
    """
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
    """
    Extracts polygons from a Placemark. Handles multiple Polygon elements.
    (If you have MultiGeometry with polygons, this still works via .//kml:Polygon)
    """
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

        # close ring if needed
        if coords[0] != coords[-1]:
            coords.append(coords[0])

        poly = Polygon(coords)
        if poly.is_empty or not poly.is_valid:
            continue

        polys.append(poly)

    return polys


@dataclass
class SpatialIndex:
    tree: STRtree
    geom_id_to_value: Dict[int, str]


def build_zone_indexes_from_paths(zone_kml_paths: List[str]) -> Dict[str, SpatialIndex]:
    """
    Creates a dict: sector -> SpatialIndex
    Each polygon maps to ZoneFull = "Sector - ZoneName".
    """
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
        tree = STRtree(geoms)
        geom_id_to_value = {id(g): v for g, v in zip(geoms, by_sector_vals[sector])}
        indexes[sector] = SpatialIndex(tree=tree, geom_id_to_value=geom_id_to_value)

    return indexes


def build_freguesia_index_from_path(freguesias_kml_path: str) -> SpatialIndex:
    """
    Builds an index mapping polygons -> freguesia name.
    Uses iterparse for large files.
    Your file uses <SimpleData name="Freguesia">NAME</SimpleData>.
    """
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
    return SpatialIndex(tree=tree, geom_id_to_value=geom_id_to_value)


def lookup_point(index: SpatialIndex, lat: float, lon: float) -> Optional[str]:
    """
    Returns mapped value if the point is inside/covered by a polygon, else None.
    """
    pt = Point(float(lon), float(lat))  # shapely uses x=lon, y=lat
    candidates = index.tree.query(pt)
    for geom in candidates:
        if geom.covers(pt):  # includes boundary points
            return index.geom_id_to_value.get(id(geom))
    return None
