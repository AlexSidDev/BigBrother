from frontend.streamlit_ui import StreamlitUI
from frontend.db_connector import DBConnectionHandler
import time

if __name__ == "__main__":
    UI = StreamlitUI()
    UI.run()
    # obj = DBConnectionHandler()

    # while True:
    #     obj.get_updates()
    #     time.sleep(5)
