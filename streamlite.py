import streamlit as st
import pandas as pd
import numpy as np
import os
import requests

def infer_postgres_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "bigint"
    if pd.api.types.is_float_dtype(dtype):
        return "double precision"
    if pd.api.types.is_bool_dtype(dtype):
        return "boolean"
    return "text"

def generate_create_table_query(table_name, df):
    column_defs = []
    for col in df.columns:
        safe_col = col.lower().replace(' ', '_')
        pg_type = infer_postgres_type(df[col])
        column_defs.append(f'"{safe_col}" {pg_type}')
    columns_sql = ',\n    '.join(column_defs)
    sql = f'''
    CREATE TABLE IF NOT EXISTS public.{table_name} (
        id serial PRIMARY KEY,
        {columns_sql}
    );
    '''
    return sql

# Streamlit starts here
st.title("Upload and Save CSV to Supabase (with Table Creation)")

uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    st.write("Data preview:")
    st.dataframe(df)

    filename, _ = os.path.splitext(uploaded_file.name)
    table_name = filename.lower().replace(' ', '_')
    st.write(f"Destination table: **{table_name}**")

    # Build SQL CREATE statement
    create_table_sql = generate_create_table_query(table_name, df)

    if st.button(f"Create/Update Table and Insert Data"):
        # 1. Create table on Supabase using SQL endpoint
        SUPABASE_URL = st.secrets["SUPABASE_URL"]
        SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
        sql_url = SUPABASE_URL.replace('.co', '.co/rest/v1/rpc/execute_sql')

        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        }
        resp = requests.post(
            sql_url,
            headers=headers,
            json={"sql": create_table_sql}
        )

        if resp.ok:
            st.info("Table created or already exists. Now inserting data...")

            # 2. Prepare DataFrame and insert data
            df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
            from supabase import create_client
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            data = df.to_dict(orient="records")
            response = supabase.table(table_name).insert(data).execute()
            if response.status_code == 201:
                st.success(f"Data successfully saved to table {table_name}!")
            else:
                st.error(f"Error saving data: {response.status_code}")
        else:
            st.error(f"Failed to create/alter table: {resp.text}")
