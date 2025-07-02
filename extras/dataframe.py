import streamlit as st
import pandas as pd
import json
import os

st.title("All JSON Files as Tables")

# Change this to your folder path
root_folder = "my_data_folder"

# Walk through all subfolders and get JSON files
for dirpath, dirnames, filenames in os.walk(root_folder):
    for file in filenames:
        if file.endswith(".json"):
            file_path = os.path.join(dirpath, file)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # Normalize if nested JSON
                if isinstance(data, dict):
                    df = pd.json_normalize(data)
                elif isinstance(data, list):
                    df = pd.json_normalize(data)
                else:
                    df = pd.DataFrame([data])

                st.subheader(f"📄 {file}")
                st.dataframe(df)

            except Exception as e:
                st.error(f"❌ Error reading {file_path}: {e}")
import streamlit as st
import pandas as pd
import json
import os

st.title("All JSON Files as Tables")

# Change this to your folder path
root_folder = "my_data_folder"

# Walk through all subfolders and get JSON files
for dirpath, dirnames, filenames in os.walk(root_folder):
    for file in filenames:
        if file.endswith(".json"):
            file_path = os.path.join(dirpath, file)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # Normalize if nested JSON
                if isinstance(data, dict):
                    df = pd.json_normalize(data)
                elif isinstance(data, list):
                    df = pd.json_normalize(data)
                else:
                    df = pd.DataFrame([data])

                st.subheader(f"📄 {file}")
                st.dataframe(df)

            except Exception as e:
                st.error(f"❌ Error reading {file_path}: {e}")