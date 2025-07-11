import streamlit as st
import datetime
from logic import run_all
import csv
import io

st.set_page_config(layout="wide")
st.title("Publication Metrics Generator (Pythonic)")

# File uploaders
df_1_file = st.file_uploader("Upload Publication Report (df_1)", type="csv")
df_2_file = st.file_uploader("Upload Author ID Report (df_2)", type="csv")

# Date inputs
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", datetime.date(2024, 7, 1))
with col2:
    end_date = st.date_input("End Date", datetime.date(2025, 6, 30))

# Helper to convert list of dicts to CSV string
def dicts_to_csv(dict_list):
    if not dict_list:
        return ""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=dict_list[0].keys())
    writer.writeheader()
    writer.writerows(dict_list)
    return output.getvalue()

# Run logic when button is clicked
if st.button("Run Analysis"):
    if not (df_1_file and df_2_file):
        st.error("❌ Please upload both files.")
    else:
        with st.spinner("⏳ Processing..."):
            try:
                df_8_list, df_11_list = run_all(df_1_file, df_2_file, start_date, end_date)
                st.success("✅ Analysis complete!")

                # Convert results to CSV
                df_8_csv = dicts_to_csv(df_8_list)
                df_11_csv = dicts_to_csv(df_11_list)

                # Download buttons
                st.download_button("Download df_8 (Authorship Summary)", df_8_csv, file_name="df_8.csv", mime="text/csv")
                st.download_button("Download df_11 (Total Publications)", df_11_csv, file_name="df_11.csv", mime="text/csv")

                # Show preview
                st.subheader("Preview of df_8")
                st.dataframe(df_8_list)

                st.subheader("Preview of df_11")
                st.dataframe(df_11_list)

            except Exception as e:
                st.error(f"❌ An error occurred: {e}")
