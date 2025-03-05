import streamlit as st


class Visualizer:
    def __init__(self):
        pass

    def barplot_NER_distribution(self, statistic: dict):
        st.bar_chart(statistic, x_label="NER tags")
