import streamlit as st
import datetime
import pandas as pd

from streamlit_option_menu import option_menu

from .db_connector import DBConnectionHandler
from .visualizer import Visualizer


class StreamlitUI:
    def __init__(self) -> None:
        self.db_handler = DBConnectionHandler("data/labeled_dataset.csv")
        self.visualizer = Visualizer()
        self.pages = {"General statistic": self.home_page,
                      "N twits": self.colorful_twits_for_category_page,
                      "Relevant NERs": self.relevant_NERs}

        self.days = {"All the time": 0, "Last day": 1,
                     "Last week": 7, "Last month": 30, "Last year": 365}

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
        number_of_tweets = self.db_handler.get_number_of_twits()
        st.write("Number of collected tweets:", number_of_tweets)
        self.NER_statistic_page()
        self.NER_sentiment_page()

    def NER_statistic_page(self):
        st.write("## NER statistic")
        st.markdown(
            """
            In this section you can see NER-tags distribution for selected period.
            """
        )
        period = st.selectbox("Select period", self.days.keys())
        statistc = self.db_handler.get_NER_distrubution(self.days[period])
        self.visualizer.barplot(statistc, "NER tags")

    def NER_sentiment_page(self):
        st.write("## Sentiment distribution")
        st.markdown(
            """
            In this section you can see sentiment distribution for named entity categories.
            """
        )
        NER_tags = self.db_handler.get_NER_tags()
        selected_tag = st.selectbox("Select named entity category", NER_tags)
        statistc = self.db_handler.get_sentiment_statistic_for_NER(
            selected_tag)
        self.visualizer.barplot(statistc, "Sentiment tags")

    def colorful_twits_for_category_page(self):
        st.write("## Twits topics")
        st.markdown(
            """
            In this section you can see twits with colored named entites for selected topic and dates.
            """
        )
        catogories = self.db_handler.get_categories()
        selected_category = st.selectbox(f"Select topic", catogories)
        dates = self.visualizer.dates_selection(
            self.db_handler.min_date, self.db_handler.start, self.db_handler.today)

        data = pd.DataFrame()
        if len(dates) == 2:
            start_period, end_period = dates
            data = self.db_handler.get_n_twits_for_categoty(
                selected_category, start_period, end_period)

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
            self.db_handler.min_date, self.db_handler.start, self.db_handler.today)
        relevant_ner_df = pd.DataFrame()
        if len(dates) == 2:
            start_period, end_period = dates
            relevant_ner_df = self.db_handler.get_relevant_NER(
                start_period, end_period)

        if len(relevant_ner_df):
            selected_entity = None
            n_rows = len(relevant_ner_df)
            # replace to visualizer
            cols = st.columns(n_rows, vertical_alignment="center")
            for index, row in relevant_ner_df.iterrows():
                with cols[index]:
                    if st.button(row.entity, use_container_width=True):
                        selected_entity = row.entity

            if (selected_entity):
                data = self.db_handler.get_rows_with_certain_token(
                    selected_entity, start_period, end_period)
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
                NER_stat = self.db_handler.count_NER_distribution(data)
                self.visualizer.barplot(NER_stat, "NER tags")
                st.markdown(
                    """
                    \n\n
                    #### Sentiment distribution for selected entity:
                    """
                )
                statistic = data["sentiment_labels"].value_counts().to_dict()
                self.visualizer.barplot(statistic, "Sentiment tags")
        else:
            st.markdown("#### No twits in selected dates")


if __name__ == "__main__":
    UI = StreamlitUI()
    UI.run()
