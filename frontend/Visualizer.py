import streamlit as st


class Visualizer:
    def __init__(self):
        pass

    def barplot(self, statistic: dict, descripltion: str):
        st.bar_chart(statistic, x_label=descripltion)
