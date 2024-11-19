import pandas as pd
import numpy as np

class DataProcessor:
    
    @staticmethod
    def to_dataframe(df: pd.DataFrame) -> pd.DataFrame:

        df = pd.DataFrame(df)

        return df
    
    @staticmethod
    def dtype_to_sql(df_dict: dict) -> dict:

        sql_type = {}
        df_type = {}

        mapping = {
             np.object_: "VARCHAR(100)",
             np.float64: "FLOAT(10,2)",
             np.int64: "INT",
             np.datetime64: "DATE"
        }

        df_type = {key : df.dtypes.apply(lambda x: x.type).to_dict() for key, df in df_dict.items()}

        sql_type = {inner_key : mapping.get(inner_value) 
                    for outer_key, inner_dict in df_type.items() 
                    for inner_key, inner_value in inner_dict.items()}
        
        sql_formatted = {column_name.replace(" ", "_") : column_type for column_name, column_type in sql_type.items()}

        return sql_formatted

    @staticmethod
    def format_date(df: pd.DataFrame, column_date: str, date_format: str) -> pd.DataFrame:

        try:
            df[column_date] = pd.to_datetime(df[column_date], format = 'mixed')
            df[column_date] = df[column_date].dt.strftime(date_format)
            df[column_date] = pd.to_datetime(df[column_date], format = 'mixed')
            print("Date formatted successfully.")

            return df

        except Exception as e:
            print(e)

    @staticmethod
    def save_csv(df: pd.DataFrame, path: str) -> None:

        try:
            df.to_csv(path, index = False)
            print("Data saved successfully as a csv file.")

        except Exception as e:
            print(e)

    @staticmethod
    def extract_csv_data(path: str) -> list:

        df = pd.read_csv(path)

        data_list = [tuple(row) for i, row in df.iterrows()]

        return data_list

