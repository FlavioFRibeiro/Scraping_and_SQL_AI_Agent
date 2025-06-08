import streamlit as st
from ai_agent_analyzer import analise_sql_agente_ia

st.set_page_config(page_title="SQL AI Agent", layout="centered")

st.title("SQL AI Agent - Consulta Inteligente ao Banco de dados")

st.write("Faça uma pergunta sobre os dados dos livros (ex: 'Qual o preço médio dos livros?').")

question = st.text_input("Pergunta:", "")

if st.button("Enviar Pergunta") and question.strip():
    with st.spinner("Consultando o agente..."):
        resposta = analise_sql_agente_ia(question)
    st.markdown("**Resposta do Agente:**")
    st.write(resposta)