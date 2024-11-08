def add_ireland_to_address(df, address_column):
    df[address_column] = df[address_column] + ',' + df['county'] + ',Ireland'
    return df

def preprocess_raw_data_for_join(df, address):
    """Takes the raw DF from the PPR and updates the address to match the
      format from the output data (i.e. input string)"""
    df2 = add_ireland_to_address(df, address)
    input_string = df2[address]
    df_done = df.copy()
    df_done.loc[:,"input_string"] = input_string
    return df_done

def join_input_and_output(input_df, output_df):
    input_plus_output = input_df.merge(output_df, left_on='input_string', right_on='input_string', suffixes=("_input", "_output"))
    return input_plus_output
