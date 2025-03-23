from frontend.streamlit_ui import StreamlitUI
from frontend.db_connector import DBConnectionHandler
import time

if __name__ == "__main__":
    UI = StreamlitUI()
    UI.run()
