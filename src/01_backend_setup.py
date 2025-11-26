import duckdb
import pandas as pd
import requests
import json
from pyproj import Transformer

# DB setup
DB_FILE = 'buildings_database.db'
MUNI_FILE = 'municipalities_zh.geojson'

# Bounding Box in RD New:
minx = 78600.0
miny = 445000.0
maxx = 85800.0
maxy = 450000.0
bbox_28992 = (minx, miny, maxx, maxy)

def transform_to_wgs84(input_bbox, input_crs="EPSG:28992"):
    print(f"Original Bounding Box (EPSG:28992): {bbox_28992}")

    transformer = Transformer.from_crs(input_crs, "EPSG:4326", always_xy=True)
    xmin_rd, ymin_rd, xmax_rd, ymax_rd = input_bbox

    lon_min, lat_min = transformer.transform(xmin_rd, ymin_rd)
    lon_max, lat_max = transformer.transform(xmax_rd, ymax_rd)

    return lon_min, lat_min, lon_max, lat_max


def download_pdok_municipalities(geojson_file):
    """
    Downloads Zuid-Holland municipality boundaries from PDOK in EPSG:28992.
    """
    print(f"Downloading Zuid-Holland municipalities to {geojson_file}...")

    url = "https://api.pdok.nl/kadaster/bestuurlijkegebieden/ogc/v1_0/collections/gemeentegebied/items"

    params = {
        "f": "json",
        "crs": "http://www.opengis.net/def/crs/EPSG/0/28992",
        "ligt_in_provincie_naam": "Zuid-Holland",
        "limit": 1000
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()

        with open(geojson_file, 'w', encoding='utf-8') as f:
            json.dump(response.json(), f)

        print("Download successful.")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading from PDOK: {e}")
        raise


def download_overture_data(db_file):
    """
    Connects to a DuckDB file, executes the SQL query to download Overture Maps data.
    """
    print(f"Connecting to or creating database file: **{db_file}**")

    # Establish a connection to the DuckDB file
    # If the file doesn't exist, it will be created.
    with duckdb.connect(database=db_file) as conn:
        print("Executing SQL query to download and import data...")

        sql_query = f"""
        INSTALL spatial;
        LOAD spatial;
        INSTALL httpfs;
        LOAD httpfs;

        -- Overture Maps data is hosted in us-west-2
        SET s3_region='us-west-2';

            CREATE OR REPLACE TABLE overture_buildings AS
            SELECT
                id,
                bbox,
                ST_Transform(ST_FLIPCOORDINATES(geometry), 'EPSG:4326', 'EPSG:28992') AS geometry
            FROM
                read_parquet('s3://overturemaps-us-west-2/release/2025-10-22.0/theme=buildings/type=building/*.parquet')
            WHERE
                bbox.xmin BETWEEN {XMIN} AND {XMAX}
                AND bbox.ymin BETWEEN {YMIN} AND {YMAX}
        """
        conn.execute(sql_query)

        results = conn.execute("SELECT count(*) FROM overture_buildings;").fetchone()

        if results:
            print(f"Imported **{results[0]}** buildings.")
        else:
            print("Some error occurred during data import.")


def print_db(db_file):
    # Connect to the persistent database file
    with duckdb.connect(database=db_file) as conn:
        # display columns names
        columns = conn.execute("PRAGMA table_info('overture_buildings');").fetchall()
        column_names = [col[1] for col in columns]
        print("Column names in 'overture_buildings' table:", column_names)

        # Run a SELECT query and fetch the result directly into a Pandas DataFrame
        data_df = conn.sql("SELECT * FROM overture_buildings LIMIT 20;").df()

        print(data_df)


def join_municipalities(db_file, geojson_path):
    """
    Loads the local GeoJSON of municipalities and spatially joins it with buildings.
    """
    print("Performing spatial join with municipalities...")

    sql_query = f"""
    INSTALL spatial;
    LOAD spatial;

    CREATE OR REPLACE TABLE municipalities AS 
    SELECT * FROM ST_Read('{geojson_path}');

    ALTER TABLE overture_buildings ADD COLUMN IF NOT EXISTS municipality_name VARCHAR;

    UPDATE overture_buildings
    SET municipality_name = m.naam
    FROM municipalities m
    WHERE ST_Intersects(overture_buildings.geometry, m.geom); 
    """


    with duckdb.connect(database=db_file) as conn:
        conn.execute(sql_query)

        # Verification
        result = conn.execute("""
        SELECT municipality_name, count(*)
        FROM overture_buildings
        GROUP BY municipality_name
        """).df()
        print("Buildings per Municipality:")
        print(result)

if __name__ == "__main__":
    # Set pandas to display full rows and columns
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    XMIN, YMIN, XMAX, YMAX = transform_to_wgs84(bbox_28992)
    print(f"Transformed Bounding Box (EPSG:4326): {XMIN, YMIN, XMAX, YMAX}")

    download_overture_data(DB_FILE)

    download_pdok_municipalities(MUNI_FILE)

    join_municipalities(DB_FILE, MUNI_FILE)

    print_db(DB_FILE)
