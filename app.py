import streamlit as st
import pandas as pd
import datetime
from logic import run_all

st.set_page_config(layout="wide")
st.title("Publication Metrics Generator")

df_1_file = st.file_uploader("Upload Publication Report (df_1)", type="csv")
df_2_file = st.file_uploader("Upload Author ID Report (df_2)", type="csv")
api_key = st.text_input("Enter Scopus API Key", type="password")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", datetime.date(2024, 7, 1))
with col2:
    end_date = st.date_input("End Date", datetime.date(2025, 6, 30))

if st.button("Run Analysis"):
    if not (df_1_file and df_2_file and api_key):
        st.error("Please upload both files and provide an API key.")
    else:
        df_1 = pd.read_csv(df_1_file)
        df_2 = pd.read_csv(df_2_file)

        with st.spinner("Processing..."):
            df_8, df_11 = process_publications(df_1, df_2, api_key, start_date, end_date)

        st.success("Done!")
        st.download_button("Download df_8 (Authorship Summary)", df_8.to_csv(index=False), "df_8.csv")
        st.download_button("Download df_11 (Total Publications)", df_11.to_csv(index=False), "df_11.csv")

        st.subheader("Preview df_8")
        st.dataframe(df_8)

        st.subheader("Preview df_11")
        st.dataframe(df_11)
