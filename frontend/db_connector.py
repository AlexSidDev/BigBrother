import datetime
import pandas as pd
import re
import ast
import nltk

from backend.keyword.extraction import KeywordExtractor
from backend.kafka.database_managers import DatabaseReader


class DBConnectionHandler:
    # Currently we use .csv as DB
    def __init__(self, csv_path) -> None:
        self.db_connector = DatabaseReader()

        self.mwtokenizer = nltk.MWETokenizer([tuple('{{') + ('URL',) + tuple('}}'),
                                              tuple('{{') + ('USERNAME',) + tuple('}}'),
                                              tuple("{@"), tuple("@}")],
                                             separator='')

        self.df = self._preprocess(self.db_connector.get_all())

        self.NER_tags_list = ['corporation', 'creative_work',
                              'event', 'group', 'location', 'person', 'product']
        self.sentiment_tags_list = ['negative', 'positive', 'neutral']
        self.categories_list = {"world": "LABEL_0",
                                "sport": "LABEL_1",
                                "business": "LABEL_2",
                                "sci-tech": "LABEL_3"}
        self.extractor = KeywordExtractor(self.df)

        self.min_date = self.get_min_date()
        self.today = datetime.datetime.now()
        self.start = self.today - datetime.timedelta(days=7)

    def _preprocess(self, data_rows) -> pd.DataFrame:
        data_rows = list(map(lambda entity: entity._mapping, data_rows))
        data = pd.DataFrame(data_rows)

        def preprocess_twits(twit):
            splitted_tweet = nltk.word_tokenize(twit)
            twit = self.mwtokenizer.tokenize(splitted_tweet)
            return twit

        data["tokens"] = data["tweet"].apply(preprocess_twits)
        data["NER_labels"] = data["NER_labels"].apply(
            lambda tag: ast.literal_eval(tag) if type(tag) == str else tag)
        data["date"] = pd.to_datetime(data["date"]).dt.date

        return data

    def count_NER_distribution(self, df):
        labels_dict = dict(
            zip(self.NER_tags_list, [0] * len(self.NER_tags_list)))
        for tokens_str in df['NER_labels']:
            for token in tokens_str:
                if token.startswith('B'):
                    labels_dict[token.split('-')[-1]] += 1

        return labels_dict

    def get_NER_distrubution(self, number_of_days_in_period: int) -> dict:
        # Use date form the past for debbuging
        # Uncomment for using actual dates
        # today = datetime.datetime.today()
        today = datetime.date(2020, 5, 17)
        start = today - datetime.timedelta(days=number_of_days_in_period)

        if (number_of_days_in_period):
            filtered_data = self.df[(self.df['date'] > start) & (
                self.df['date'] <= today)]
        else:
            filtered_data = self.df

        return self.count_NER_distribution(filtered_data)

    def get_number_of_twits(self) -> int:
        return len(self.df)

    def get_sentiment_statistic_for_NER(self, NER_tag) -> dict:
        filtered_data = self.df[self.df['NER_labels'].apply(
            lambda x: NER_tag in ' '.join(x))]
        return filtered_data["sentiment_labels"].value_counts().to_dict()

    def get_n_twits_for_categoty(self, category: str, start_period: datetime.datetime, end_period: datetime.datetime, N=10) -> pd.DataFrame():
        filtered_data = self.df[(self.df['date'] >= start_period)]
        filtered_data = filtered_data[(filtered_data['date'] <= end_period)]
        filtered_data = filtered_data[self.df['categ_labels']
                                      == self.categories_list[category]]
        if len(filtered_data) < N:
            return filtered_data
        return filtered_data.sample(N)

    def get_NER_tags(self) -> list:
        return self.NER_tags_list

    def get_categories(self) -> list:
        return self.categories_list.keys()

    def get_relevant_NER(self, start_date, end_date) -> pd.DataFrame:
        result = self.extractor.extract_relevant(start_date, end_date)
        return pd.DataFrame(result)

    def get_min_date(self) -> datetime.datetime:
        return self.df["date"].min()

    def get_rows_with_certain_token(self, token: str, start_period: datetime.datetime, end_period: datetime.datetime):
        filtered_data = self.df[self.df.tokens.apply(
            lambda x: token in ' '.join(x))]
        filtered_data = filtered_data[(filtered_data['date'] >= start_period)]
        filtered_data = filtered_data[(filtered_data['date'] <= end_period)]
        return filtered_data
