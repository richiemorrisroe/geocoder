def add_ireland_to_address(df, address_column):
    addresses = (df[address_column] + ',' + df['county'] + ',Ireland').tolist()
    return addresses

def preprocess_raw_data_for_join(df, address):
    """Takes the raw DF from the PPR and updates the address to match the
      format from the output data (i.e. input string)"""
    input_string = add_ireland_to_address(df, address)
    df_done = df.copy()
    df_done.loc[:,"input_string"] = input_string
    return df_done

def join_input_and_output(input_df, output_df):
    input_plus_output = input_df.join(output_df, lsuffix="_input", rsuffix="_output")
    return input_plus_output
