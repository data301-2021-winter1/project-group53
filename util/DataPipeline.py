import pandas as pd

class DataPipeline():
    __DATASET_PATH: str
    __OUTPUT_PATH: str
    __DATAFRAME: pd.DataFrame

    def __init__(self, dataset_path: str, output_path: str):
        self.__DATASET_PATH = dataset_path
        self.__OUTPUT_PATH = output_path
        self.__DATAFRAME = self.__read_dataset()
    
    def get_dataset_path(self) -> str:
        return self.__DATASET_PATH

    def get_output_path(self) -> str:
        return self.__OUTPUT_PATH
    
    def get_dataset_path_and_output_path(self) -> tuple:
        return self.__DATASET_PATH, self.__OUTPUT_PATH
    
    def dataset_stats(self) -> tuple:
        """
        Returns:
            tuple: (info, describe, head, tail, columns, dtypes, missing, count by weather condition, correlation)
        """
        return self.__DATAFRAME.info(), self.__DATAFRAME.describe(), self.__DATAFRAME.head(), self.__DATAFRAME.tail(), self.__DATAFRAME.columns, self.__DATAFRAME.dtypes, self.__DATAFRAME.isnull().sum(), self.__DATAFRAME.groupby('Weather_Condition').count(), self.__DATAFRAME.corr()
    
    def __read_dataset(self) -> pd.DataFrame:
        return pd.read_csv(self.__DATASET_PATH)

    def __clean_dataset(self):
        """
        Clean dataset.

        Executes
        --------
        1. Remove rows with missing values
        2. Remove duplicates
        3. Remove unnecessary columns
        """
        self.__DATAFRAME.dropna(inplace=True)
        self.__DATAFRAME.drop_duplicates(inplace=True)
        self.__DATAFRAME.drop([
            'Start_Lat', 'Start_Lng', 'End_Lat', 'End_Lng', 'Distance(mi)', 'Number', 'Street', 'Side', 'City', 'County', 'State', 'Zipcode', 'Country', 'Timezone', 'Airport_Code', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop'
        ], axis=1, inplace=true)

    def write_dataset(self, output_path: str = None) -> None:
        """
        Export dataframe to CSV

        Args:
            output_path (str, optional): Defaults to None.

        """
        if output_path is None:
            self.__DATAFRAME.to_csv(self.__OUTPUT_PATH, index=False)
        else:
            self.__DATAFRAME.to_csv(output_path, index=False)
