from typing import List

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder

from steps.src.data_loader import DataLoader


class CategoricalEncoder:
    """
    This class applies encoding to categorical variables. 
    
    Parameters
    ----------
    method: str, default="onehot"
        The method to encode the categorical variables. Can be "onehot" or "ordinal".
    
    categories: 'auto' or a list of lists, default='auto'
        Categories for the encoders. Must match the number of columns. If 'auto', categories are determined from data.
    """
    def __init__(self, method: str ="onehot", categories: str ="auto"):
        self.method = method
        self.categories = categories
        self.encoders = {}
        
    def fit(self, df: pd.DataFrame, columns) -> None:
        for col in columns:
            if self.method == "onehot":
                self.encoders[col] = OneHotEncoder(sparse=False, categories=self.categories)
            elif self.method == "ordinal":
                self.encoders[col] = OrdinalEncoder(categories=self.categories)
            else:
                raise ValueError(f"Invalid method. Please use one of 'onehot' or 'ordinal'.")
            # Encoders expect 2D input data
            self.encoders[col].fit(df[[col]])

    def transform(self, df, columns):
        df_encoded = df.copy()
        for col in columns:
            # Encoders expect 2D input data
            transformed = self.encoders[col].transform(df[[col]])
            if self.method == "onehot":
                # OneHotEncoder returns a 2D array, we need to convert it to a DataFrame
                transformed = pd.DataFrame(transformed, columns=self.encoders[col].get_feature_names_out([col]))
                df_encoded = pd.concat([df_encoded.drop(columns=[col]), transformed], axis=1)
            else:
                df_encoded[col] = transformed
        return df_encoded
       

    def fit_transform(self, df, columns):
        self.fit(df, columns)
        return self.transform(df, columns)



