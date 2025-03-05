import streamlit as st

from DBConnectionHandler import DBConnectionHandler
from Visualizer import Visualizer


class StreamlitUI:
    def __init__(self) -> None:
        self.db_handler = DBConnectionHandler("data/labeled_dataset.csv")
        self.visualizer = Visualizer()
        self.pages = {"Home": self.home_page,
                      "NER statistic": self.NER_statistic_page}

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

    def NER_statistic_page(self):
        st.write("# Page with NER statistic...")
        period = st.selectbox("Select time period", self.days.keys())
        statistc = self.db_handler.get_NER_distrubution(self.days[period])
        self.visualizer.barplot_NER_distribution(statistc)


if __name__ == "__main__":
    UI = StreamlitUI()
    UI.run()
