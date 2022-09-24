import spatialite


def create_schema(df):
    cols = df.columns
    col_str = """,""".join([c for c in cols])
    return col_str

def create_table(db, table_name, schema):
    with spatialite.connect(db) as con:
        cursor = con.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})")
    return 1


def create_connection(db_name:str):
    con = spatialite.connect(db_name)
    return con
