import duckdb
import json
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Path, Query, Request
from pydantic import BaseModel

# --- CONFIGURATION ---
DB_FILE = 'buildings_database.db'
LIMIT_DEFAULT = 50
LIMIT_MAX = 1000


# --- MODELS ---

class CollectionLink(BaseModel):
    href: str
    rel: str
    type: str = "application/geo+json"
    title: str


class CollectionItem(BaseModel):
    id: str
    title: str
    description: str
    itemType: str = "feature"
    building_count: int
    links: List[CollectionLink]


class CollectionsResponse(BaseModel):
    collections: List[CollectionItem]


class GeoJSONFeature(BaseModel):
    type: str = "Feature"
    geometry: Dict[str, Any]
    properties: Dict[str, Any]


class GeoJSONFeatureCollection(BaseModel):
    type: str = "FeatureCollection"
    numberMatched: int  # Total count
    numberReturned: int  # Count of features in this response
    limit: int
    offset: int
    links: List[CollectionLink]
    features: List[GeoJSONFeature]


# --- API SETUP ---
app = FastAPI()


# --- UTILITIES ---

def run_db_query(query: str, params: tuple = ()) -> List[tuple]:
    """Execute read-only DuckDB query."""
    try:
        with duckdb.connect(database=DB_FILE, read_only=True) as conn:
            conn.execute("INSTALL spatial;")
            conn.execute("LOAD spatial;")
            return conn.execute(query, params).fetchall()
    except Exception as e:
        raise HTTPException(500, detail=f"DB Error: {e}")


def create_geojson_feature_from_row(row: tuple, cols: List[str]) -> GeoJSONFeature:
    """
    Converts a DuckDB row into a GeoJSON Feature.
    """
    col_map = {name: i for i, name in enumerate(cols)}

    building_id = str(row[col_map['id']])
    municipality_name = row[col_map['municipality_name']]

    properties = {
        "id": building_id,
        "municipality_name": municipality_name,
    }

    geometry_json = json.loads(row[col_map['geometry_geojson']])

    return GeoJSONFeature(
        properties=properties,
        geometry=geometry_json
    )


def create_pagination_links(request: Request, total_count: int, limit: int, offset: int) -> List[CollectionLink]:
    """Generates next/prev links preserving current query parameters."""
    base_url = str(request.url).split('?')[0]
    query_params = dict(request.query_params)
    links = []

    # Self link
    links.append(CollectionLink(href=str(request.url), rel="self", title="Current page"))

    # Next link
    if offset + limit < total_count:
        query_params['offset'] = offset + limit
        query_params['limit'] = limit
        # Reconstruct query string
        qs = "&".join(f"{k}={v}" for k, v in query_params.items())
        links.append(CollectionLink(href=f"{base_url}?{qs}", rel="next", title="Next page"))

    # Prev link
    if offset > 0:
        prev_offset = max(0, offset - limit)
        query_params['offset'] = prev_offset
        query_params['limit'] = limit
        qs = "&".join(f"{k}={v}" for k, v in query_params.items())
        links.append(CollectionLink(href=f"{base_url}?{qs}", rel="prev", title="Previous page"))

    return links


# --- ENDPOINTS ---

@app.get("/ping")
def ping():
    return {"status": "ok"}

@app.get("/collections", response_model=CollectionsResponse)
def list_collections(request: Request):
    """List all available municipalities and building counts."""
    query = """
        SELECT municipality_name, count(*)
        FROM overture_buildings
        WHERE municipality_name IS NOT NULL
        GROUP BY municipality_name
        ORDER BY count(*) DESC;
    """

    results = run_db_query(query)

    base_path = request.base_url

    collection_items = []
    for municipality, count in results:
        collection_items.append(
            CollectionItem(
                id=municipality.replace("'", "").replace(" ", "-"),
                title=f"Buildings in {municipality}",
                description=f"Building footprints for {municipality} municipality",
                building_count=count,
                links=[
                    CollectionLink(
                        href=f"{base_path}collections/{municipality}/items",
                        rel="items",
                        type="application/geo+json",
                        title=f"Buildings in {municipality}"
                    )
                ]
            )
        )

    return CollectionsResponse(collections=collection_items)


@app.get("/collections/{municipality}/items", response_model=GeoJSONFeatureCollection)
def get_municipality_items(
        request: Request,
        municipality: str,
        limit: int = Query(LIMIT_DEFAULT, ge=1, le=LIMIT_MAX),
        offset: int = Query(0, ge=0)
):
    """Get all buildings in a municipality."""

    # 1. Get Total Count
    count_query = "SELECT count(*) FROM overture_buildings WHERE municipality_name = ?;"
    total_count = run_db_query(count_query, (municipality,))[0][0]

    # 2. Fetch Data and Geometry
    data_query = """
        SELECT
            id,
            municipality_name,
            ST_AsGeoJSON(geometry) AS geometry_geojson
        FROM overture_buildings
        WHERE municipality_name = ?
        LIMIT ?
        OFFSET ?;
    """

    data_cols = ['id', 'municipality_name', 'geometry_geojson']
    results = run_db_query(data_query, (municipality, limit, offset))

    features = [create_geojson_feature_from_row(row, data_cols) for row in results]
    links = create_pagination_links(request, total_count, limit, offset)

    return GeoJSONFeatureCollection(
        type="FeatureCollection",
        numberMatched=total_count,
        numberReturned=len(features),
        limit=limit,
        offset=offset,
        links=links,
        features=features
    )


@app.get("/collections/{municipality}/items/{building_id}", response_model=GeoJSONFeature)
def get_specific_building(municipality: str, building_id: str):
    """Get a specific building."""

    data_query = """
        SELECT
            id,
            municipality_name,
            ST_AsGeoJSON(geometry) AS geometry_geojson
        FROM overture_buildings
        WHERE id = ? AND municipality_name = ?;
    """

    data_cols = ['id', 'municipality_name', 'geometry_geojson']
    results = run_db_query(data_query, (building_id, municipality))

    if not results:
        raise HTTPException(404, detail="Building not found.")

    return create_geojson_feature_from_row(results[0], data_cols)


@app.get("/buildings/bbox", response_model=GeoJSONFeatureCollection)
def query_by_bbox(
        request: Request,
        minx: float = Query(...),
        miny: float = Query(...),
        maxx: float = Query(...),
        maxy: float = Query(...),
        limit: int = Query(LIMIT_DEFAULT, ge=1, le=LIMIT_MAX),
        offset: int = Query(0, ge=0)
):
    """Return all buildings within the given bounding box (EPSG:28992)."""

    bbox_polygon_wkt = (
        f"POLYGON(({minx} {miny}, {maxx} {miny}, {maxx} {maxy}, {minx} {maxy}, {minx} {miny}))"
    )

    bbox_filter = f"ST_Intersects(geometry, ST_GeomFromText('{bbox_polygon_wkt}'))"

    count_query = f"SELECT count(*) FROM overture_buildings WHERE {bbox_filter};"
    total_count = run_db_query(count_query)[0][0]


    data_query = f"""
        SELECT
            id,
            municipality_name,
            ST_AsGeoJSON(geometry) AS geometry_geojson
        FROM overture_buildings
        WHERE {bbox_filter}
        LIMIT ? OFFSET ?;
    """

    data_cols = ['id', 'municipality_name', 'geometry_geojson']
    results = run_db_query(data_query, (limit, offset))

    features = [create_geojson_feature_from_row(row, data_cols) for row in results]
    links = create_pagination_links(request, total_count, limit, offset)

    return GeoJSONFeatureCollection(
        type="FeatureCollection",
        numberMatched=total_count,
        numberReturned=len(features),
        limit=limit,
        offset=offset,
        links=links,
        features=features
    )
