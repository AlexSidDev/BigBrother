import ast
from datetime import datetime
import pandas as pd
import json

import sys
sys.path.append('/home/polina/BigBrother')

from backend.kafka.database_managers import DatabaseWriter


class PreFiller:
    def __init__(self,
                 labeled_data_path: str = "data/sorted_labeled_dataset.csv",
                 config_path: str = "configs/application_config.json"):
        self.writer = DatabaseWriter()
        self.data = pd.read_csv(labeled_data_path, converters={'tokens': ast.literal_eval})
        self.data['time'] = self.data['date'].apply(PreFiller.convert_data)
        self.data['tweet'] = self.data['tokens'].apply(str)
        self.data.drop(columns=['tokens', 'date', 'id'], inplace=True)
        with open(config_path, 'r') as config:
            self.start_date = PreFiller.convert_data(json.load(config)["start_date"])

    @staticmethod
    def convert_data(str_date: str):
        return datetime.strptime(str_date, "%Y-%m-%d")

    def fill(self):
        data_to_add = self.data[self.data['time'] < self.start_date]
        data_to_add = data_to_add.to_dict(orient='records')
        self.writer.add_rows(data_to_add)


if __name__ == '__main__':
    prefiller = PreFiller()
    prefiller.fill()
