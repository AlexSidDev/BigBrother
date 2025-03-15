import streamlit as st

from DBConnectionHandler import DBConnectionHandler
from Visualizer import Visualizer


class StreamlitUI:
    def __init__(self) -> None:
        self.db_handler = DBConnectionHandler("data/labeled_dataset.csv")
        self.visualizer = Visualizer()
        self.pages = {"Home": self.home_page,
                      "NER statistic": self.NER_statistic_page,
                      "NER Sentiment": self.NER_sentiment_page,
                      "N twits": self.colorful_twits_for_category_page}

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


if __name__ == "__main__":
    UI = StreamlitUI()
    UI.run()
