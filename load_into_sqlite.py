import argparse

import pandas as pd

import geocode.sql as sql
import geocode.geocode_funcs as gf

parser = argparse.ArgumentParser()
parser.add_argument("--database")
parser.add_argument("--file-path")
parser.add_argument("--table-name")
parser.add_argument("--if-exists")
parser.add_argument("--filetype")
parser.add_argument("--process", default=False)
args = parser.parse_args()

def main(args):
    print(f"{args=}")
    connection = sql.create_connection(args.database)
    try:
        if args.filetype == "shapefile":
            sql.load_shapefile(args.file_path, args.table_name)
            
        if args.filetype == "csv":
            try:
                data = pd.read_csv(args.file_path)
            except Exception as e:
                # probably latin-1
                data = pd.read_csv(args.file_path, encoding='latin1')
            
        if args.filetype == "feather":
            data = pd.read_feather(args.file_path)
        
        if not data.empty and args.process:
            data2 = gf.remove_duplicates(data)
            data_standardised  = gf.standardise_data(data2, address_column='address')
            print(f"{data_standardised.columns=}")
            sql.load_data_into_table(connection,
                                     table_name = args.table_name,
                                     data = data_standardised,
                                     if_exists=args.if_exists)
        if not data.empty and not args.process:
            sql.load_data_into_table(connection,
                                     table_name = args.table_name,
                                     data = data,
                                     if_exists=args.if_exists)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main(args)










