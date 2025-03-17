import streamlit as st
import datetime
import pandas as pd

from .db_connector import DBConnectionHandler
from .visualizer import Visualizer


class StreamlitUI:
    def __init__(self) -> None:
        self.db_handler = DBConnectionHandler("data/labeled_dataset.csv")
        self.visualizer = Visualizer()
        self.pages = {"Home": self.home_page,
                      "NER statistic": self.NER_statistic_page,
                      "NER Sentiment": self.NER_sentiment_page,
                      "N twits": self.colorful_twits_for_category_page,
                      "Relevant NERs": self.relevant_NERs}

        self.days = {"All the time": 0, "Last day": 1,
                     "Last week": 7, "Last month": 30, "Last year": 365}

    def run(self) -> None:
        page = st.sidebar.radio("Select page", list(self.pages.keys()))
        self.pages[page]()

    def home_page(self):
        st.write("# Big Brother is watching you...")
        st.markdown(
            """
        This is an application for data visualisation of twits.
        """
        )
        number_of_tweets = self.db_handler.get_number_of_twits()
        st.write("Number of collected tweets:", number_of_tweets)

    def NER_statistic_page(self):
        st.write("# Page with NER statistic...")
        period = st.selectbox("Select time period", self.days.keys())
        statistc = self.db_handler.get_NER_distrubution(self.days[period])
        self.visualizer.barplot(statistc, "NER tags")

    def NER_sentiment_page(self):
        NER_tags = self.db_handler.get_NER_tags()
        selected_tag = st.selectbox("Select NER tag", NER_tags)
        statistc = self.db_handler.get_sentiment_statistic_for_NER(
            selected_tag)
        self.visualizer.barplot(statistc, "Sentiment tags")

    def colorful_twits_for_category_page(self):
        catogories = self.db_handler.get_categories()
        selected_category = st.selectbox("Select category", catogories)
        data = self.db_handler.get_n_twits_for_categoty(
            selected_category)

        self.visualizer.visualize_categories()

        for index, row in data.iterrows():
            self.visualizer.colorfu_text(row["tokens"], row["NER_labels"])

    def relevant_NERs(self):
        min_date = self.db_handler.get_min_date()
        today = datetime.datetime.now()
        start = today - datetime.timedelta(days=7)
        dates = st.date_input(
            "Select period",
            (start, today),
            min_date,
            today,
            format="YYYY/MM/DD",
        )
        relevant_ner_df = pd.DataFrame()
        if len(dates) == 2:
            start_period, end_period = dates
            relevant_ner_df = self.db_handler.get_relevant_NER(
                start_period, end_period)

        if len(relevant_ner_df):
            selected_entity = None
            n_rows = len(relevant_ner_df)
            cols1 = st.columns(n_rows//2)
            cols2 = st.columns(n_rows - n_rows//2)
            for index, row in relevant_ner_df[:n_rows//2].iterrows():
                button_key = "button_" + str(index)
                with cols1[index]:
                    if st.button(row.entity):
                        selected_entity = row.entity

            for index, row in relevant_ner_df[n_rows//2:n_rows].iterrows():
                button_key = "button_" + str(index)
                with cols2[index - n_rows//2]:
                    if st.button(row.entity):
                        selected_entity = row.entity

            if (selected_entity):
                data = self.db_handler.get_rows_with_certain_token(
                    selected_entity, start_period, end_period)
                NER_stat = self.db_handler.count_NER_distribution(data)
                self.visualizer.barplot(NER_stat, "NER tags")


if __name__ == "__main__":
    UI = StreamlitUI()
    UI.run()
