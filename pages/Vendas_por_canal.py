import pandas as pd
import streamlit as st
from io import BytesIO
import streamlit_authenticator as stauth
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import numpy as np

from FacebookAds import get_data_from_bucket


try:
    sales_journeys = st.session_state['sales_journeys']
except:
    sales_journeys = pd.read_feather(BytesIO(get_data_from_bucket(bucket_name='dashboard_marketing_processed', file_name='sales_journeys.feather', file_type='.feather')))
    sales_journeys['approved_date'] = sales_journeys['approved_date'].apply(lambda x: pd.to_datetime(x[0]))
    st.session_state['sales_journeys'] = sales_journeys

try:
    hotmart = st.session_state['hotmart_data']
except:
    tmp_hotmart = get_data_from_bucket(bucket_name='dashboard_marketing_processed', file_name='processed_hotmart.parquet', file_type='.parquet')
    raw_hotmart = pd.read_parquet(BytesIO(tmp_hotmart), engine='pyarrow')
    raw_hotmart['count'] = 1
    raw_hotmart['tracking.source_sck'] = raw_hotmart['tracking.source_sck'].fillna(value='Desconhecido')
    raw_hotmart['tracking.source'] = raw_hotmart['tracking.source'].fillna(value='Desconhecido')
    raw_hotmart['approved_date'] = pd.to_datetime(raw_hotmart['approved_date'])
    st.session_state['hotmart_data'] = raw_hotmart
    hotmart = st.session_state['hotmart_data'] = raw_hotmart


from collections import defaultdict
@st.cache_data
def get_revenue_by_source(user_journey_with_revenue: pd.DataFrame) -> pd.DataFrame:
    """
    Adds revenue by source to the DataFrame.
    """
    # Make a copy of the DataFrame to avoid modifying the original
    user_journey = user_journey_with_revenue.copy()
    user_journey['revenue_per_source'] = user_journey['commission.value'] / user_journey['utm_source_std'].apply(len)
    # Initialize dictionary to store revenue by source
    revenue_by_source = {}

    # Iterate over each row in the DataFrame
    for idx, row in user_journey.iterrows():
        # Iterate over sources in the utm_source_std list
        for source in row['utm_source_std']:
            # Update revenue for each unique source
            revenue_by_source.setdefault(source, 0)
            revenue_by_source[source] += row['revenue_per_source']

    # Convert dictionary to DataFrame
    revenue_df = pd.DataFrame(list(revenue_by_source.items()), columns=['utm_source_std', 'total_revenue'])
    revenue_df['total_revenue'] = revenue_df['total_revenue'].apply(lambda x: float(x[0]))
    return revenue_df





#######################################################################
authenticator = stauth.Authenticate(
    dict(st.secrets['credentials']),
    st.secrets['cookie']['name'],
    st.secrets['cookie']['key'],
    st.secrets['cookie']['expiry_days'],
    st.secrets['preauthorized']
)

name, authentication_status, username = authenticator.login('Login', 'main')
if st.session_state["authentication_status"]:
    authenticator.logout('Logout', 'sidebar')
    st.title('Vendas por canal - Visão Geral')

    ##################### FILTERS ##########################################

    valid_hotmart = hotmart.loc[hotmart['status'].isin(['APPROVED','COMPLETE'])]
    today = datetime.today()
    date_range = st.sidebar.date_input("Periodo atual", value=(pd.to_datetime(valid_hotmart['order_date']).max()-timedelta(days=6), pd.to_datetime(valid_hotmart['order_date']).max()), max_value=pd.to_datetime(valid_hotmart['order_date']).max(), min_value=pd.to_datetime(valid_hotmart['order_date']).min())

    limited_sales = sales_journeys.loc[(sales_journeys['approved_date'].dt.date >= date_range[0]) & (sales_journeys['approved_date'].dt.date <= date_range[1])]
    limited_hotmart = valid_hotmart.loc[(valid_hotmart['approved_date'].dt.date >= date_range[0]) & (valid_hotmart['approved_date'].dt.date <= date_range[1])]
    

    revenue_by_source = get_revenue_by_source(user_journey_with_revenue=limited_sales)   
    target = limited_hotmart.loc[(limited_hotmart['source'] == 'PRODUCER'), 'commission.value'].sum()
    
    
    col_1, col_2 = st.columns(2)
    with col_1: 
            st.subheader('% Faturamento Indentificado')
            target = limited_hotmart.loc[(limited_hotmart['status'].isin(['APPROVED','COMPLETE']))
                                        & (limited_hotmart['source'] == 'PRODUCER'), 'commission.value'].sum()
            target_fig = go.Figure()
            target_fig.add_trace(trace=go.Indicator(mode = "gauge+number+delta", value = round(target/float(revenue_by_source['total_revenue'].sum()) * 100,0),
                                                    title = {'text': "Faturamento Identificado"}, delta={'reference':100},
                                                    gauge={'shape': 'bullet',
                                                            'axis': {'range': [0, 100]}}
                                                    ))
            st.plotly_chart(target_fig, use_container_width=True)
       
    with col_2:
        sources_fig = px.pie(data_frame=revenue_by_source, names='utm_source_std', values='total_revenue', title='Distribuição do faturamento')
        st.plotly_chart(sources_fig, use_container_width=True)
