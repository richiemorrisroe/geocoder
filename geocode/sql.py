import spatialite
import pandas as pd


def create_schema(df):
    cols = df.columns
    col_str = """,""".join([c for c in cols])
    return col_str

def create_table(con, table_name, schema):
    with con as con:
        cursor = con.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})")
    return table_name


def create_connection(db_name:str):
    con = spatialite.connect(db_name)
    return con

def load_data_into_table(con, table_name, data:pd.DataFrame, if_exists=None):
    if not if_exists:
        raise ValueError("please define behaviour when table already exists")
    data.to_sql(name=table_name, con=con, if_exists="replace")
    return 1

def load_shapefile(con, shapefile_path, table_name):
    if not table_name:
        raise ValueError("table name must be supplied")
    projection = "CP1252"
    cursor = con.cursor()
    cursor.execute(f""".loadshp electoral_divisions_gps {table_name} {projection}""")
    return 1
