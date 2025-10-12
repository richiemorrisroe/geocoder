import logging
import sqlite3 as spatialite
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


def create_connection(db_name: str):
    con = spatialite.connect(db_name)
    return con


def load_data_into_table(con, table_name, data: pd.DataFrame, if_exists=None):
    if not if_exists:
        raise ValueError("please define behaviour when table already exists")
    print(f"{data.head()=}")
    data.to_sql(name=table_name, con=con, if_exists=if_exists)
    return 1


def append_to_table(con, table_name: str, data: pd.DataFrame):
    res = load_data_into_table(con, table_name, data, if_exists="append")
    return True


def load_shapefile(shapefile_path, table_name):
    if not table_name:
        raise ValueError("table name must be supplied")
    projection = "CP1252"
    import subprocess

    subprocess.call(
        [
            "spatialite",
            "property.db",
            f".loadshp electoral_divisions_gps {table_name} {projection}",
        ]
    )
    return 1


def join_tables(con, table_left, table_right, join_type, join_keys):
    pass

def drop_table(con, table_name):
    con.execute(f"drop table if exists {table_name}")
    return None


def get_data_from_db(connection, query):
    result = pd.read_sql(query, con=connection)
    return result


def get_property_data(connection, table_name, num_results):
    query = f"select * from {table_name} limit {num_results}"
    result = get_data_from_db(connection, query)
    return result


def generate_ungeocoded_addresses(connection):
    return None


def check_for_new_rows(connection, table_name, limit=None, county_name=None, from_date=None):
    if limit:
        limit_str = f"LIMIT {limit}"
    else:
        limit_str = ""
    if county_name:
        county_str = f"AND county='{county_name}'"
    else:
        county_str = ""
    if from_date:
        date_str = f"AND stg.date_of_sale > '{from_date}'"
    else:
        date_str = ""

    sql = """SELECT stg.*
    FROM property_sales_stg stg
    LEFT JOIN property_sales_geocoded gc
    ON
    stg.unique_id=gc.unique_id
    WHERE
    latitude is null
    {county_str}
    {date_str}
    {limit}""".format(
        limit=limit_str, county_str=county_str, date_str=date_str
    )
    logging.log(level=1, msg=f"{sql=}")
    data = get_data_from_db(connection, query=sql)
    return data


def format_results(results):
    # assert len(result) == 1, 'Cannot handle multiple objects'-
    res = []
    for input_string, results in results.items():
        res.append(results)
    res_df = pd.DataFrame.from_records(res)
    return res_df


def check_for_missing_rows(conn, table_a, table_b):
    query = f"select * from {table_a} a LEFT JOIN {table_b} b USING(address, date_of_sale, price) where b.unique_id is null"
    res = get_data_from_db(conn, query)
    # breakpoint()
    return res
