import duckdb
import pandas as pd
from pyproj import Transformer

# DB setup
DB_FILE = 'buildings_database.db'
LIMIT = 100

# Bounding Box in RD New:
minx = 78600.0
miny = 445000.0
maxx = 85800.0
maxy = 450000.0
bbox_28992 = (minx, miny, maxx, maxy)

def transform_to_wgs84(input_bbox, input_crs="EPSG:28992"):
    print(f"Original Bounding Box (EPSG:28992): {bbox_28992}")

    transformer = Transformer.from_crs("EPSG:28992", "EPSG:4326", always_xy=True)
    xmin_rd, ymin_rd, xmax_rd, ymax_rd = input_bbox

    lon_min, lat_min = transformer.transform(xmin_rd, ymin_rd)
    lon_max, lat_max = transformer.transform(xmax_rd, ymax_rd)

    return lon_min, lat_min, lon_max, lat_max


def download_overture_data(db_file: str, sql_query: str):
    """
    Connects to a DuckDB file, executes the SQL query to download Overture Maps data,
    and saves it to a table.
    """
    print(f"Connecting to or creating database file: **{db_file}**")

    # Establish a connection to the DuckDB file
    # If the file doesn't exist, it will be created.
    with duckdb.connect(database=db_file) as conn:
        print("Executing SQL query to download and import data...")

        # Execute the entire multi-statement SQL query
        conn.execute(sql_query)

        results = conn.execute("SELECT count(*) FROM delft_buildings;").fetchone()

        if results:
            print(f"Imported **{results[0]}** buildings for Delft.")
        else:
            print("Some error occurred during data import.")


def print_db():
    # Connect to the persistent database file
    with duckdb.connect(database=DB_FILE) as conn:
        # display columns names
        columns = conn.execute("PRAGMA table_info('delft_buildings');").fetchall()
        column_names = [col[1] for col in columns]
        print("Column names in 'delft_buildings' table:", column_names)

        # Run a SELECT query and fetch the result directly into a Pandas DataFrame
        data_df = conn.sql("SELECT id, height, geometry FROM delft_buildings LIMIT 20;").df()

        print(data_df)

if __name__ == "__main__":
    # Set pandas to display full rows and columns
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    XMIN, YMIN, XMAX, YMAX = transform_to_wgs84(bbox_28992)
    print(f"Transformed Bounding Box (EPSG:4326): {XMIN, YMIN, XMAX, YMAX}")

    SQL_QUERY = f"""
    INSTALL spatial;
    LOAD spatial;
    INSTALL httpfs;
    LOAD httpfs;

    -- Overture Maps data is hosted in us-west-2
    SET s3_region='us-west-2';

    CREATE OR REPLACE TABLE delft_buildings AS
    SELECT
      *
    FROM
      read_parquet('az://overturemapswestus2.blob.core.windows.net/release/2025-10-22.0/theme=buildings/type=building/*', filename=true, hive_partitioning=1)
    WHERE
      names.primary IS NOT NULL
      AND bbox.xmin BETWEEN {XMIN} AND {XMAX}
      AND bbox.ymin BETWEEN {YMIN} AND {YMAX}
    LIMIT {LIMIT};
    """

    try:
        download_overture_data(DB_FILE, SQL_QUERY)
    except Exception as e:
        print(f"An error occurred during the operation: {e}")

    print_db()
