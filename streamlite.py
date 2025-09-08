import streamlit as st
import pandas as pd
import numpy as np
from supabase import create_client

# Inicializa conexão Supabase e mantém com cache
@st.cache_resource
def init_supabase():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = init_supabase()

st.title("Upload e Salvar CSV no Supabase")

# Upload do arquivo CSV
uploaded_file = st.file_uploader("Escolha um arquivo CSV", type="csv")

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    st.write("Visualização dos dados carregados:")
    st.dataframe(df)

    if st.button("Salvar no Supabase (tabela raw)"):
        data = df.where(pd.notnull(df), None).to_dict(orient="records")
        response = supabase.table("raw").insert(data).execute()
        if response.status_code == 201:
            st.success("Dados salvos com sucesso na tabela raw do Supabase!")
        else:
            st.error(f"Erro ao salvar dados: {response.status_code}")

