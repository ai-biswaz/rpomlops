import pandas as pd
from zenml import step
from zenml.logger import get_logger

from steps.src.data_processor import CategoricalEncoder
from steps.src.feature_engineering import DateFeatureEngineer

logger = get_logger(__name__)

@step
def categorical_encode(df: pd.DataFrame)  -> pd.DataFrame:
    try:
        encoder = CategoricalEncoder(method="onehot")
        df = encoder.fit_transform(df, columns=["product_id", "product_category_name"])
        logger.info("Successfully encoded categorical variables.")
        return df
    except Exception as e:
        logger.error("Error while encoding categorical variables.")
        raise e


@step
def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """
    Performs feature engineering on the data.
    
    Args:
        df (pd.DataFrame): Input DataFrame to be processed.
        
    Returns:
        pd.DataFrame: DataFrame after feature engineering.
    """
    try:
        # Apply date feature engineering
        date_engineer = DateFeatureEngineer(date_format="%d-%m-%Y")
        df_transformed = date_engineer.fit_transform(df, ["month_year"])

        # Log the successful operation
        logger.info("Successfully enginnered features.") 

        # Drop unnecessary columns
        df_transformed.drop(["id", "month_year"], axis=1, inplace=True)

        return df_transformed

    except Exception as e:
        logger.error("Error while engineering features.")
        raise e
    
