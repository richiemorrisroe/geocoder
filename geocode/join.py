from .geocode_funcs import add_ireland_to_address


def preprocess_raw_data_for_join(df, address):
    """Takes the raw DF from the PPR and updates the address to match the
      format from the output data (i.e. input string)"""
    df_done = add_ireland_to_address(df, address)
    return df_done

def join_input_and_output():
    pass
