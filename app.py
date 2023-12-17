import streamlit as st

st.set_page_config(page_title="Youtube Data Harvesting and Warehousing", page_icon=':bar_chart:', layout='wide')

# header section
st.title("Youtube Dataharvesting and Warehousing:")

#all the current workign options
st.subheader("Please read and select the options listed below: ")
st.markdown("1. Retrieve data from YouTube")
st.markdown("2. Store data to MongoDB")
st.markdown("3. Migrating data to SQL data warehouse")
st.markdown("4. Data Analysis")
st.markdown("5. Executing SQL queries")
st.markdown("6. Exit")

#dropdown
option = st.selectbox('Select One',
                      ('Retrieve data from YouTube', 
                       'Store data to MongoDB', 
                       'Migrating data to SQL data warehouse', 
                       "Migrating data to SQL data warehouse",
                       "Data Analysis",
                       "Executing SQL queries",
                       'Exit'
                      ))