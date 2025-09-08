import streamlit as st
import pandas as pd
import numpy as np
import os
from supabase import create_client

@st.cache_resource
def init_supabase():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = init_supabase()
st.title("Upload and Save CSV to Supabase")

uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    st.write("Data preview:")
    st.dataframe(df)
    # Get table name from file name (without extension)
    table_name = os.path.splitext(uploaded_file.name)[0].lower().replace(' ', '_')
    st.write(f"Destination table: **{table_name}**")

    if st.button(f"Save to Supabase (table {table_name})"):
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        data = df.to_dict(orient="records")
        response = supabase.table(table_name).insert(data).execute()
        if response.status_code == 201:
            st.success(f"Data successfully saved to table {table_name}!")
        else:
            st.error(f"Error saving data: {response.status_code}")
