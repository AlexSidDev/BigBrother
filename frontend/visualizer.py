import streamlit as st
import re
import ast
import pandas as pd


class Visualizer:
    def __init__(self):
        self.colors = {
            "person": "#ADD8E6",  # Голубой
            "corporation": "#32CD32",     # Светло-зеленый
            "creative_work": "#FF69B4",     # Светло-розовый
            "event": "#FFA500",    # Золотой
            "group": "#8A2BE2",   # Фиолетовый
            "location": "#DC143C",  # Малиновый
            "product": "#4682B4",     # Стальной синий
        }

        self.colors_light = {
            # Бледно-голубой (полупрозрачный)
            "person": "rgba(176, 224, 230, 0.3)",
            # Бледно-зеленый (полупрозрачный)
            "corporation": "rgba(152, 251, 152, 0.3)",
            # Бледно-розовый (полупрозрачный)
            "creative_work": "rgba(255, 182, 193, 0.3)",
            # Бледно-золотой (полупрозрачный)
            "event": "rgba(255, 215, 0, 0.3)",
            # Бледно-фиолетовый (полупрозрачный)
            "group": "rgba(155, 48, 255, 0.3)",
            # Бледно-малиновый (полупрозрачный)
            "location": "rgba(255, 99, 71, 0.3)",
            # Бледно-стальной синий (полупрозрачный)
            "product": "rgba(135, 206, 250, 0.3)",
        }

    def barplot(self, statistic: dict, x_descripltion: str, y_descripltion: str):
        st.bar_chart(statistic, x_label=x_descripltion,  y_label=y_descripltion,horizontal=True)

    def colorful_text(self, words: list[str], ner_tags: list[str]):
        styled_text = ""
        for word, tag in zip(words, ner_tags):
            tag = tag.split('-')[-1]
            color = self.colors.get(tag, "black")  # По умолчанию черный
            if color == "black":
                styled_text += word + " "
            else:
                styled_text += f'<span style="color: {color}; font-weight: bold;">{word} </span>'
        styled_block = f"""
        <div style="background-color: #F5F5F5; padding: 10px; border-radius: 5px;">
            {styled_text}
        </div>
        """

        st.markdown(styled_block, unsafe_allow_html=True)
        st.markdown("\n")

    def visualize_categories(self):
        st.markdown("#### Colors of named entity labels in twits:")
        keys = list(self.colors.keys())

        cols = st.columns(len(keys))

        for index, key in enumerate(keys):
            with cols[index]:
                st.markdown(f"""
                                <div style="
                                    background-color: {self.colors_light[key]};
                                    color: gray;
                                    padding: 15px;
                                    border-radius: 10px;
                                    text-align: center;
                                    font-size: 14px;
                                    width: 220px;
                                    margin: auto;">
                                    {key}
                                </div>
                            """, unsafe_allow_html=True)

    def dates_selection(self, min_date, start, end, max_date):
        dates = st.date_input(
            "Select period",
            (start, end),
            min_date,
            max_date,
            format="YYYY/MM/DD",
        )
        return dates

    def visualize_selected_twits(self, data: pd.DataFrame, min_cols_num=1, max_cols_num=5):
        st.markdown(
            "\n\n")

        n_cols = min(max(len(data), min_cols_num), max_cols_num)
        cols = st.columns(n_cols)
        for index, row in data.reset_index().iterrows():
            with cols[index % n_cols]:
                st.write(f"##### { pd.to_datetime(row['time']).strftime('%Y-%m-%d')}")
                self.colorful_text(row['tokens'], row['ner'])
                st.write(f"")
