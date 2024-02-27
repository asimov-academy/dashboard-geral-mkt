import pandas as pd
import streamlit as st
from io import BytesIO
from datetime import datetime, timedelta
import plotly.graph_objects as go
from millify import millify
import plotly.express as px

from FacebookAds import get_data_from_bucket


try:
    hotmart = st.session_state['hotmart_data']
except:
    tmp_hotmart = get_data_from_bucket(bucket_name='dashboard_marketing_processed', file_name='processed_hotmart.parquet', file_type='.parquet')
    raw_hotmart = pd.read_parquet(BytesIO(tmp_hotmart), engine='pyarrow')
    raw_hotmart['count'] = 1
    raw_hotmart['tracking.source_sck'] = raw_hotmart['tracking.source_sck'].fillna(value='Desconhecido')
    raw_hotmart['approved_date'] = pd.to_datetime(raw_hotmart['approved_date'])
    st.session_state['hotmart_data'] = raw_hotmart
    hotmart = st.session_state['hotmart_data'] = raw_hotmart

try:
    ga4 = st.session_state['ga4']
except:
    tmp_ga4 = get_data_from_bucket(bucket_name='dashboard_marketing_processed', file_name='ga4_data_dash.parquet', file_type='.parquet')
    raw_ga4 = pd.read_parquet(BytesIO(tmp_ga4), engine='pyarrow')
    raw_ga4['transaction_id'].replace(to_replace='', value='N/A', inplace=True)
    st.session_state['ga4'] = raw_ga4
    ga4 = st.session_state['ga4']

try:
    valid_ga4 = st.session_state['valid_ga4']
except:
    valid_ga4 = ga4.loc[ga4['event_date'].isin(hotmart['order_date'])] 
    valid_ga4['utm_source_std'] = valid_ga4.apply(lambda x: 'Direct' if (x['utm_source_std'] == 'Others') & (x['default_channel'] == 'Direct') else x['utm_source_std'], axis=1)
    
    # First, filter hotmart transactions based on 'tracking.source_sck'
    hotmart_filtered_transactions = hotmart.loc[hotmart['tracking.source_sck'].str.contains('venda'), 'transaction']

    # Then, filter valid_ga4 to include only rows where 'utm_source_std' is 'Direct' and the 'transaction_id' is in the filtered hotmart transactions
    valid_ga4_filtered = valid_ga4[(valid_ga4['utm_source_std'] == 'Direct') & (valid_ga4['transaction_id'].isin(hotmart_filtered_transactions))]

    # Group valid_ga4_filtered by 'ga_session_id' and update the 'utm_source_std' column for each group
    valid_ga4['utm_source_std'] = valid_ga4.groupby('ga_session_id')['utm_source_std'].transform(lambda x: 'Vendas' if all(x == 'Direct') else x)
    st.session_state['valid_df'] = valid_ga4


@st.cache_data
def get_user_journey(ga4_data: pd.DataFrame) -> pd.DataFrame:
    """
    Return a DataFrame containing the default channel, transaction_id, and all UTM parameters from all sessions in which a user,
    whose user_id is present in a session with a purchase, participated.
    """
    sessions_with_purchases = ga4_data[ga4_data['event_name'] == 'purchase']['ga_session_id'].unique()
    sessions_data = ga4_data.loc[(ga4_data['user_inferred_id'].isin(ga4_data.loc[ga4_data['ga_session_id'].isin(sessions_with_purchases), 'user_inferred_id']))
                                 & ~(ga4_data['transaction_id'].isin(['','(not set)',None]))]

    user_journey = sessions_data.groupby('user_inferred_id').agg({
        'utm_source_std': lambda x: list(x.unique()),
        'default_channel': lambda x: list(x.unique()),
        'utm_campaign': lambda x: list(x.unique()),
        'utm_content': lambda x: list(x.unique()),
        'transaction_id': lambda x: list(x.unique())
    }).reset_index()

    user_journey.columns = ['user_inferred_id', 'utm_source_std', 'default_channel', 'utm_campaign', 'utm_content', 'transaction_id']
    return user_journey

def add_revenue_to_journey(user_journey_df: pd.DataFrame, hotmart_data: pd.DataFrame) -> pd.DataFrame:
    """
    Adds to the dataframe user_journey the revenue associated with each user
    """
    # Make a copy of user_journey_df to avoid modifying the original DataFrame
    user_journey_with_revenue = user_journey_df.copy()
    
    # Filter hotmart_data to include only approved and complete transactions from producer source
    valid_transactions = hotmart_data[(hotmart_data['status'].isin(['APPROVED', 'COMPLETE'])) & (hotmart_data['source'] == 'PRODUCER')]
    
    # Group hotmart_data by transaction and calculate the total commission value for each transaction
    transaction_revenue = valid_transactions.groupby('transaction')['commission.value'].sum().reset_index()
    
    # Explode the 'transaction' column to create separate rows for each transaction
    user_journey_exploded = user_journey_with_revenue.explode('transaction_id')
    
    # Merge user_journey_exploded with transaction_revenue based on transaction ID
    user_journey_with_revenue = pd.merge(user_journey_exploded, transaction_revenue, left_on='transaction_id', right_on='transaction', how='left')
    
    # Calculate revenue per user by dividing total commission value by the number of users in the journey
    user_journey_with_revenue['total_revenue'] = user_journey_with_revenue['commission.value'] / user_journey_with_revenue.groupby('transaction_id')['user_inferred_id'].transform('count')
    
    # Drop unnecessary columns
    user_journey_with_revenue.drop(['commission.value'], axis=1, inplace=True)
    
    return user_journey_with_revenue

def get_revenue_by_source(user_journey_wrevenue: pd.DataFrame, sources: list) -> dict:
    """
    Returns the total revenue by source in user_jorney_wrevenue. Sales with more than one source will have their value equally divided
    """
    revenue_by_source = dict.fromkeys(sources, 0)  # Initialize the dictionary with default value 0

    for user in user_journey_wrevenue['user_inferred_id'].unique():
        user_sources = list(user_journey_wrevenue.loc[user_journey_wrevenue['user_inferred_id'] == user, 'utm_source_std'].values)
        for source in user_sources:
            source = ''.join(source) #flatten the f** list
            if source == 'Facebook + Instagram':
                revenue_by_source['Facebook + Instagram'] += user_journey_wrevenue.loc[user_journey_wrevenue['user_inferred_id'] == user, 'total_revenue'].sum() / len(user_sources)
            elif source == 'Google':
                revenue_by_source['Google'] += user_journey_wrevenue.loc[user_journey_wrevenue['user_inferred_id'] == user, 'total_revenue'].sum() / len(user_sources)
            elif source == 'Direct':
                revenue_by_source['Direct'] += user_journey_wrevenue.loc[user_journey_wrevenue['user_inferred_id'] == user, 'total_revenue'].sum() / len(user_sources)
            elif source == 'YouTube':
                revenue_by_source['YouTube'] += user_journey_wrevenue.loc[user_journey_wrevenue['user_inferred_id'] == user, 'total_revenue'].sum() / len(user_sources)
            elif source == 'Active Campaign':
                revenue_by_source['Active Campaign'] += user_journey_wrevenue.loc[user_journey_wrevenue['user_inferred_id'] == user, 'total_revenue'].sum() / len(user_sources)               
            elif source == 'Bing':
                revenue_by_source['Bing'] += user_journey_wrevenue.loc[user_journey_wrevenue['user_inferred_id'] == user, 'total_revenue'].sum() / len(user_sources)
            elif source == 'Whatsapp':
                revenue_by_source['Whatsapp'] += user_journey_wrevenue.loc[user_journey_wrevenue['user_inferred_id'] == user, 'total_revenue'].sum() / len(user_sources)
            elif source == 'Blog':
                revenue_by_source['Blog'] += user_journey_wrevenue.loc[user_journey_wrevenue['user_inferred_id'] == user, 'total_revenue'].sum() / len(user_sources)
            elif source == 'Tik Tok':
                revenue_by_source['Tik Tok'] += user_journey_wrevenue.loc[user_journey_wrevenue['user_inferred_id'] == user, 'total_revenue'].sum() / len(user_sources)
            elif source == 'LinkedIn':
                revenue_by_source['LinkedIn'] += user_journey_wrevenue.loc[user_journey_wrevenue['user_inferred_id'] == user, 'total_revenue'].sum() / len(user_sources)
            elif source == 'Hub':
                revenue_by_source['Hub'] += user_journey_wrevenue.loc[user_journey_wrevenue['user_inferred_id'] == user, 'total_revenue'].sum() / len(user_sources)
            elif source == 'Vendas':
                revenue_by_source['Vendas'] += user_journey_wrevenue.loc[user_journey_wrevenue['user_inferred_id'] == user, 'total_revenue'].sum() / len(user_sources)
            else:
                revenue_by_source['Others'] += user_journey_wrevenue.loc[user_journey_wrevenue['user_inferred_id'] == user, 'total_revenue'].sum() / len(user_sources)

    return pd.DataFrame(revenue_by_source.items(), columns=['source', 'revenue'])

#######################################################################
st.title('Vendas por canal - Visão Geral')
st.write(valid_ga4.head())
sources = ("Facebook + Instagram", 'Google', 'Direct', 'YouTube', 'Active Campaign','Others', 'Bing', 'Whatsapp', 'Blog', 'Tik Tok', 'Linkedin','Hub', 'Vendas')
##################### FILTERS ##########################################
date_range = st.sidebar.date_input("Periodo atual", value=(valid_ga4['event_date'].max() - timedelta(days=6), valid_ga4['event_date'].max()), max_value=valid_ga4['event_date'].max(), min_value=valid_ga4['event_date'].min(), key='vendas_por_canal_dates')
dates_range_benchmark = st.date_input("Periodo de para comparação", value=(valid_ga4['event_date'].max()-timedelta(days=13), valid_ga4['event_date'].max()-timedelta(days=7)), max_value=valid_ga4['event_date'].max(), min_value=valid_ga4['event_date'].min(), key='vendas_por_canal_dates_benchmark')
limited_ga4 = valid_ga4.loc[(valid_ga4['event_date'].dt.date >= date_range[0]) & (valid_ga4['event_date'].dt.date <= date_range[1])]
limited_benchmark = valid_ga4.loc[(valid_ga4['event_date'].dt.date >= dates_range_benchmark[0]) & (valid_ga4['event_date'].dt.date <= dates_range_benchmark[1])]

limited_hotmart = hotmart.loc[(hotmart['order_date'] >= date_range[0]) & 
                                (hotmart['order_date'] <= date_range[1]) & 
                                (hotmart['status'].isin(['APPROVED','REFUNDED','COMPLETE']))] #desprezando compras canceladas

benchmark = hotmart.loc[(hotmart['order_date'] >= dates_range_benchmark[0]) & 
                        (hotmart['order_date'] <= dates_range_benchmark[1]) & 
                        (hotmart['status'].isin(['APPROVED','REFUNDED','COMPLETE']))] #desprezando compras canceladas

################################## BEGIN  #################################
user_journey = get_user_journey(limited_ga4)
user_journey_wrevenue = add_revenue_to_journey(user_journey_df=user_journey, hotmart_data=limited_hotmart)
revenue_by_source = get_revenue_by_source(user_journey_wrevenue=user_journey_wrevenue, sources=sources)

col_1, col_2 = st.columns(2)
with col_1:     
        st.subheader('Faturamento Indentificado')
        target = limited_hotmart.loc[(limited_hotmart['status'].isin(['APPROVED','COMPLETE']))
                                     & (limited_hotmart['source'] == 'PRODUCER'), 'commission.value'].sum()
        target_fig = go.Figure()
        target_fig.add_trace(trace=go.Indicator(mode = "gauge+number+delta", value = revenue_by_source['revenue'].sum(),
                                                title = {'text': "Faturamento Identificado"}, delta={'reference':target},
                                                gauge={'shape': 'bullet'}
                                                ))
        st.plotly_chart(target_fig, use_container_width=True)
        st.write(f'target {target} - total {revenue_by_source["revenue"].sum()}')
with col_2:
    sources_fig = px.pie(data_frame=revenue_by_source, names='source', values='revenue', title='Distribuição do faturamento')
    st.plotly_chart(sources_fig, use_container_width=True)
    
    
    st.write(limited_ga4.loc[(limited_ga4['utm_source_std'] == 'Direct')
                  & (limited_ga4['transaction_id'].isin(
                                                        hotmart.loc[hotmart['tracking.source_sck'].str.contains('venda'), 'transaction']
                                                     ))].groupby(by='ga_session_id', observed=True).agg(list))