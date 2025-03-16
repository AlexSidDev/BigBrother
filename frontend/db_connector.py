import datetime
import pandas as pd
import re
import ast

from backend.keyword.extraction import KeywordExtractor


class DBConnectionHandler:
    # Currently we use .csv as DB
    def __init__(self, csv_path) -> None:
        self.df = self._preprocess(csv_path)  # pd.read_csv(csv_path)

        self.NER_tags_list = ['corporation', 'creative_work',
                              'event', 'group', 'location', 'person', 'product']
        self.sentiment_tags_list = self.df["sentiment_labels"].unique()
        self.categories_list = {"world": "LABEL_0",
                                "sport": "LABEL_1",
                                "business": "LABEL_2",
                                "sci-tech": "LABEL_3"}
        self.extractor = KeywordExtractor(self.df)

    def _preprocess(self, csv_path: str) -> pd.DataFrame:
        data = pd.read_csv(csv_path)

        def preprocess_twits(twit):
            if (type(twit) == str):
                text = twit.strip("[]")
                twit = [word.strip()
                        for word in re.split(r"'(.*?)'", text) if word.strip()]
            return twit

        data["tokens"] = data["tokens"].apply(preprocess_twits)
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
        labels_dict = dict(zip(self.sentiment_tags_list, [
                           0] * len(self.sentiment_tags_list)))
        return filtered_data["sentiment_labels"].value_counts().to_dict()

    def get_n_twits_for_categoty(self, category: str, N=10) -> pd.DataFrame():

        filtered_data = self.df[self.df['categ_labels']
                                == self.categories_list[category]]

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
