import datetime
import pandas as pd


class DBConnectionHandler:
    # Currently we use .csv as DB
    def __init__(self, csv_path) -> None:
        self.df = pd.read_csv(csv_path)
        self.NER_tags_list = ['corporation', 'creative_work',
                              'event', 'group', 'location', 'person', 'product']
        self.sentiment_tags_list = self.df["sentiment_labels"].unique()
        self.categories_list = {"world": "LABEL_0",
                                "sport": "LABEL_1",
                                "business": "LABEL_2",
                                "sci-tech": "LABEL_3"}

    def get_NER_distrubution(self, number_of_days_in_period: int) -> dict:
        # Use date form the past for debbuging
        # Uncomment for using actual dates
        # today = datetime.datetime.today()
        today = datetime.datetime(2020, 5, 17)
        start = today - datetime.timedelta(days=number_of_days_in_period)
        today = today.strftime('%Y-%m-%d')
        start_date = start.strftime('%Y-%m-%d')

        if (number_of_days_in_period):
            filtered_data = self.df[(self.df['date'] > start_date) & (
                self.df['date'] <= today)]
        else:
            filtered_data = self.df

        labels_dict = dict(
            zip(self.NER_tags_list, [0] * len(self.NER_tags_list)))

        for tokens_str in filtered_data['NER_labels']:
            for token in eval(tokens_str):
                if token.startswith('B'):
                    labels_dict[token.split('-')[-1]] += 1

        return labels_dict

    def get_number_of_twits(self) -> int:
        return len(self.df)

    def get_sentiment_statistic_for_NER(self, NER_tag) -> dict:
        filtered_data = self.df[self.df['NER_labels'].str.contains(NER_tag)]
        labels_dict = dict(zip(self.sentiment_tags_list, [
                           0] * len(self.sentiment_tags_list)))
        return filtered_data["sentiment_labels"].value_counts().to_dict()
    
    def get_n_twits_for_categoty(self, category: str, N = 10) -> pd.DataFrame():
        
        filtered_data = self.df[self.df['categ_labels'] == self.categories_list[category]]

        return filtered_data.sample(N)         

    def get_NER_tags(self) -> list:
        return self.NER_tags_list
    
    def get_categories(self) -> list:
        return self.categories_list.keys()
