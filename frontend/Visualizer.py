import streamlit as st
import re
import ast


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

    def barplot(self, statistic: dict, descripltion: str):
        st.bar_chart(statistic, x_label=descripltion)

    def colorfu_text(self, words: list[str], ner_tags: list[str]):
        if (type(words) == str):
            text = words.strip("[]")
            words = [word.strip()
                     for word in re.split(r"'(.*?)'", text) if word.strip()]
        if (type(ner_tags) == str):
            ner_tags = ast.literal_eval(ner_tags)
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
        keys = list(self.colors.keys())
        half = len(keys) // 2
        col1, col2 = st.columns(2)
        legend_html = "<h4>Legend:</h4><ul style='list-style-type: none; padding: 0;'>"
        with col1:
            for tag in keys[:half + 1]:
                legend_html += f"<li style='margin-bottom: 5px;'><span style='background-color: {self.colors[tag]}; padding: 3px 8px; border-radius: 3px;'>&nbsp;&nbsp;</span> {tag}</li>"
            legend_html += "</ul>"
            st.markdown(legend_html, unsafe_allow_html=True)
        legend_html = "<h4>Legend:</h4><ul style='list-style-type: none; padding: 0;'>"
        with col2:
            for tag in keys[half + 1:]:
                legend_html += f"<li style='margin-bottom: 5px;'><span style='background-color: {self.colors[tag]}; padding: 3px 8px; border-radius: 3px;'>&nbsp;&nbsp;</span> {tag}</li>"
            legend_html += "</ul>"
            st.markdown(legend_html, unsafe_allow_html=True)
