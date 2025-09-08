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
st.title("Upload and Create/Update Table in Supabase")

uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    st.write("Data preview:")
    st.dataframe(df)
    table_name = os.path.splitext(uploaded_file.name).lower().replace(' ', '_')
    st.write(f"Destination table: **{table_name}**")

    # Create table SQL (basic: all columns as text)
    create_cols = ',\n    '.join([f'"{col}" text' for col in df.columns])
    sql_create = f'''
    CREATE TABLE IF NOT EXISTS public.{table_name} (
        id serial PRIMARY KEY,
        {create_cols}
    );
    '''
    if st.button(f"Create or update Supabase table ({table_name}) and insert data"):
        # 1. Try to create the table
        res_create = supabase.query(sql_create).execute()

        # 2. (Optional Advanced) Check/add missing columns for updates
        # Skipped for brevity; you can check info_schema.columns for this!

        # 3. Clean DataFrame for NaN/inf
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        data = df.to_dict(orient="records")
        # 4. Insert data
        response = supabase.table(table_name).insert(data).execute()
        if response.status_code == 201:
            st.success(f"Data successfully saved to table {table_name}!")
        else:
            st.error(f"Error saving data: {response.status_code}")
