import logging
import pandas as pd
from zenml import step

from steps.src.data_loader import DataLoader


@step(enable_cache=False)
def ingest_data(
    table_name: str ,
    for_predict: bool = False, 
) -> pd.DataFrame:
    """Reads data from sql database and return a pandas dataframe.

    Args:
        data: pd.DataFrame
    """
    try:
        data_loader = DataLoader('postgresql://postgres:85218521@127.0.0.1:5432/test01')
        data_loader.load_data(table_name) 
        df = data_loader.get_data()
        if for_predict:
            df.drop(columns=['unit_price'], inplace=True)
        logging.info(f"Successfully read data from {table_name}.")
        return df  
    except Exception as e:
        logging.error(f"Error while reading data from {table_name}.")
        raise e