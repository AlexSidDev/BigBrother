import datetime
import pandas as pd
import re
import ast
import nltk

from backend.keyword.extraction import KeywordExtractor
from backend.kafka.database_managers import DatabaseReader


class DBConnectionHandler:
    # Currently we use .csv as DB
    def __init__(self) -> None:
        self.db_connector = DatabaseReader()

        self.mwtokenizer = nltk.MWETokenizer([tuple('{{') + ('URL',) + tuple('}}'),
                                              tuple(
                                                  '{{') + ('USERNAME',) + tuple('}}'),
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

        self.sentiment_statistic = dict(
            (el, {"negative": 0, "neutral": 0, "positive": 0}) for el in self.NER_tags_list)
        
        self.extractor = KeywordExtractor(self.df)

        self.last_update = self.get_min_time()

        self.min_time = self.get_min_time()
        self.today = datetime.datetime.now()
        self.start = self.today - \
            datetime.timedelta(days=min(7, (self.today - self.min_time).days))

    def _preprocess(self, data) -> pd.DataFrame:
        def preprocess_twits(twit):
            splitted_tweet = nltk.word_tokenize(twit)
            twit = self.mwtokenizer.tokenize(splitted_tweet)
            return twit

        data["tokens"] = data["tweet"].apply(preprocess_twits)
        data["ner"] = data["ner"].apply(
            lambda tag: ast.literal_eval(tag) if type(tag) == str else tag)
        data["time"] = pd.to_datetime(data["time"])

        return data

    def get_updates(self):
        df = self.db_connector.read_interval(self.last_update)
        if (len(df)):
            df = self._preprocess(df)
            self.last_update = df["time"].max() + datetime.timedelta(seconds=0.5)
            self.updateSentimentStat(df)
        

    def updateSentimentStat(self, df):
        for NER_tag in self.NER_tags_list:
            filtered_data = df[df['ner'].apply(
                lambda x: NER_tag in ' '.join(x))]
            new_stat = filtered_data["sentiment"].value_counts().to_dict()
            for key, val in new_stat.items():
                self.sentiment_statistic[NER_tag][key] += val
        print(self.sentiment_statistic)

    def count_NER_distribution(self, df):
        labels_dict = dict(
            zip(self.NER_tags_list, [0] * len(self.NER_tags_list)))
        for tokens_str in df['ner']:
            for token in tokens_str:
                if token.startswith('B'):
                    labels_dict[token.split('-')[-1]] += 1

        return labels_dict

    def get_NER_distrubution(self, number_of_days_in_period: int) -> dict:
        # Use date form the past for debbuging
        # Uncomment for using actual dates
        today = datetime.datetime.today()
        # today = datetime.date(2020, 5, 17)
        start = today - datetime.timedelta(days=number_of_days_in_period)
        if (number_of_days_in_period):
            df = self.db_connector.read_interval(start, today)
            if (len(df)):
                df = self._preprocess(df)
                return self.count_NER_distribution(df)
        else:
            df = self.db_connector.get_all()
            if (len(df)):
                df = self._preprocess(df)
                return self.count_NER_distribution(df)

    def get_number_of_twits(self) -> int:
        return self.db_connector.get_count()

    def get_sentiment_statistic_for_NER(self, NER_tag) -> dict:
        return self.sentiment_statistic[NER_tag]

    def get_n_twits_for_categoty(self, category: str, start_period: datetime.datetime, end_period: datetime.datetime, N=10) -> pd.DataFrame():
        df = self.db_connector.read_interval(start_period, end_period)
        filtered_data = pd.DataFrame()
        if (len(df)):
            df = self._preprocess(df)
            print(df.columns.tolist())
            filtered_data = df[df['category'] == category]
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

    def get_min_time(self) -> datetime.datetime:
        df = self.db_connector.get_start()
        return pd.to_datetime(df["time"])

    def get_max_time(self) -> datetime.datetime:
        return self.df["time"].min()

    def get_rows_with_certain_token(self, token: str, start_period: datetime.datetime, end_period: datetime.datetime):
        df = self.db_connector.read_interval(start_period, end_period)
        filtered_data = pd.DataFrame()
        if (len(df)):
            df = self._preprocess(df)
            filtered_data = self.df[self.df.tokens.apply(
            lambda x: token in ' '.join(x))]
        return filtered_data
