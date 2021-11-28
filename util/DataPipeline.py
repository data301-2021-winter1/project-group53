import pandas as pd
import numpy as np

class DataPipeline():
    def __init__(self, elevations_data_set_path,  accidents_dataset_path: str, output_path: str):
        self._accidents_dataset_path = accidents_dataset_path
        self._elevations_data_set_path = elevations_data_set_path
        self._output_path = output_path
        self._df = self.__read_and_process_dataset()
    
    @property
    def df(self) -> pd.DataFrame:
        return self._df


    def get_dataset_path(self) -> str:
        return self._accidents_dataset_path


    def get_output_path(self) -> str:
        return self._output_path


    def get_dataset_path_and_output_path(self) -> tuple:
        return self.get_dataset_path(), self.get_output_path()


    ## Method Chaining and data wrangling
    def __read_and_process_dataset(self) -> pd.DataFrame:
        """
        Data Wrangling.

        Executes
        --------
        1. Reads Elevations Dataset
        2. Renames Latitude, Longitude to Start_Lat, Start_Long
        3. Drops Error Column
        4. Reads Accidents Dataset
        5. Drops NA
        6. Drops Duplicates
        7. Drops immecessary columns
        8. Merges datasets together on Start_Lat, Start_Long
        9. Drops NA
        10. Drops Duplicates
        """

        df_elevations = (
            pd.read_csv(self._elevations_data_set_path)
            .rename(columns={'Latitude': 'Start_Lat', 'Longitude': 'Start_Lng'})
            .drop(columns= ['Error'])
        )

        df = (
            pd.read_csv(self._accidents_dataset_path)
            .dropna()
            .drop_duplicates()
            .drop(columns=[
                    'Distance(mi)', 'Number', 'Street', 'Side', 'City', 'County', 'State', 'Zipcode', 'Country', 'Timezone', 'Airport_Code', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop'
                ])
            .merge(df_elevations, how="left", on=['Start_Lat', 'Start_Lng'])
            .replace([np.inf, -np.inf], np.nan)
            .dropna()
            .drop_duplicates()

        )

        return df


    def write_dataset(self, output_path: str = None) -> None:
        """
        Export dataframe to CSV

        Args:
            output_path (str, optional): Defaults to None.

        """
        if output_path is None:
            self._df.to_csv(self.__OUTPUT_PATH, index=False)
        else:
            self._df.to_csv(output_path, index=False)
