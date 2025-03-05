import datetime
import pandas as pd


class DBConnectionHandler:
    # Currently we use .csv as DB
    def __init__(self, csv_path) -> None:
        self.datafarme = pd.read_csv(csv_path)
        self.labels_list = ['corporation', 'creative_work',
                            'event', 'group', 'location', 'person', 'product']

    def get_NER_distrubution(self, number_of_days_in_period: int) -> dict:
        # Use date form the past for debbuging
        # Uncomment for using actual dates
        # today = datetime.datetime.today()
        today = datetime.datetime(2020, 5, 17)
        start = today - datetime.timedelta(days=number_of_days_in_period)
        today = today.strftime('%Y-%m-%d')
        start_date = start.strftime('%Y-%m-%d')

        if (number_of_days_in_period):
            filtered_data = self.datafarme[(self.datafarme['date'] > start_date) & (
                self.datafarme['date'] <= today)]
        else:
            filtered_data = self.datafarme

        labels_dict = dict(zip(self.labels_list, [0] * len(self.labels_list)))

        for tokens_str in filtered_data['NER_labels']:
            for token in eval(tokens_str):
                if token.startswith('B'):
                    labels_dict[token.split('-')[-1]] += 1

        return labels_dict
