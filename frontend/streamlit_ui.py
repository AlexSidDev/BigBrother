import streamlit as st
import datetime
import pandas as pd
import time
from streamlit_option_menu import option_menu

from .db_connector import DBConnectionHandler
from .visualizer import Visualizer

db_handler = DBConnectionHandler()
days = {"Last day": 1, "All the time": 0,
                     "Last week": 7, "Last month": 30, "Last year": 365}


class StreamlitUI:
    def __init__(self) -> None:
       self.pages = {"General statistic": self.home_page,
                     "Twits by topics": self.colorful_twits_for_category_page,
                     "Relevant NERs": self.relevant_NERs}
       self.visualizer = Visualizer()

    def run(self) -> None:
        st.set_page_config(page_title="Big Brother", layout="wide")

        page = option_menu(
            menu_title=None,
            options=list(self.pages.keys()),
            menu_icon="cast",
            default_index=0,
            orientation="horizontal"
        )

        # page = st.sidebar.radio("Select page", list(self.pages.keys()))
        self.pages[page]()

    def home_page(self):

        st.write("## Big Brother is watching you...")
        st.markdown(
            """
            This is an application for data visualisation of twits.



            """
        )

        self.placeholder = st.empty()
        self.placeholder1 = st.empty()
        NER_tags = db_handler.get_NER_tags()
        st.text("")
        st.markdown(
            """
            ## Sentiment distribution
            # you can see sentiment distribution for named entity categories.
            In this section
            """
        )
        selected_tag = st.selectbox("Select named entity category", NER_tags)
        self.placeholder2 = st.empty()
        st.write("## NER statistic")
        st.markdown(
            """
            In this section you can see NER-tags distribution for selected period.
            """
        )
        period = st.selectbox("Select period", days.keys())
        self.placeholder3 = st.empty()

        while True:
            for i in range(5):
                self.placeholder.markdown(
                    f"### Today is {db_handler.get_today().strftime('%m/%d/%Y')}")
            db_handler.get_updates()
            number_of_tweets = db_handler.get_number_of_twits()
            with self.placeholder1.container():
                st.markdown(
                    f"Number of collected tweets: **{number_of_tweets}**")
            with self.placeholder2.container():
                self.NER_sentiment_page(selected_tag)

            with self.placeholder3.container():
                self.NER_statistic_page(period)

    def NER_statistic_page(self, period):

        statistc = db_handler.get_NER_distrubution(days[period])
        self.visualizer.barplot(statistc, "Number of tags",  "NER tags")

    def NER_sentiment_page(self, selected_tag):
        statistc = db_handler.get_sentiment_statistic_for_NER(
            selected_tag)
        self.visualizer.barplot(
            statistc,  "Number of tweets", "Sentiment tags")
        # placeholder.bar_chart(statistc)

    def colorful_twits_for_category_page(self):
        st.write("## Twits topics")
        st.markdown(
            """
            In this section you can see twits with colored named entites for selected topic and dates.
            """
        )
        catogories = db_handler.get_categories()
        selected_category = st.selectbox(f"Select topic", catogories)
        dates = self.visualizer.dates_selection(
            db_handler.min_time, db_handler.start, db_handler.end, db_handler.today)

        data = pd.DataFrame()
        if len(dates) == 2:
            db_handler.start , db_handler.end = dates
            data = db_handler.get_n_twits_for_categoty(
                selected_category, db_handler.start ,  db_handler.end)

        if (len(data)):
            self.visualizer.visualize_categories()
            self.visualizer.visualize_selected_twits(data)
        else:
            st.markdown("#### No twits in selected dates")

    def relevant_NERs(self):
        
        st.markdown(
            """
            In this section you can see statistic for the most popular twits in selected dates.
            """
        )
        dates = self.visualizer.dates_selection(
            db_handler.min_time, db_handler.start, db_handler.end, db_handler.today)
        relevant_ner_df = pd.DataFrame()
        if len(dates) == 2:
            db_handler.start , db_handler.end = dates

            relevant_ner_df = db_handler.get_relevant_NER(
                db_handler.start ,  db_handler.end)

        if len(relevant_ner_df):
            selected_entity = None
            n_rows = len(relevant_ner_df)
            cols = st.columns(n_rows, vertical_alignment="center")
            for index, row in relevant_ner_df.iterrows():
                with cols[index]:
                    if st.button(row.entity, use_container_width=True):
                        selected_entity = row.entity

            if (selected_entity):
                data = db_handler.get_rows_with_certain_token(
                    selected_entity, db_handler.start,  db_handler.end)
                self.visualizer.visualize_categories()
                st.markdown(
                    """
                    \n\n
                    #### Examples of twits with selected entity:
                    """
                )
                self.visualizer.visualize_selected_twits(data)
                st.markdown(
                    """
                    \n\n
                    #### NER-tags distribution for selected entity:
                    """
                )
                NER_stat = db_handler.count_NER_distribution(data)
                self.visualizer.barplot(NER_stat, "NER tags", "Number of tags in tweets")
                st.markdown(
                    """
                    \n\n
                    #### Sentiment distribution for selected entity:
                    """
                )
                statistic = data["sentiment"].value_counts().to_dict()
                self.visualizer.barplot(statistic, "Sentiment tags", "Number of tweets")
        else:
            st.markdown("#### No twits in selected dates")


if __name__ == "__main__":
    UI = StreamlitUI()
    UI.run()
