import pandas as pd
import streamlit as st
from io import BytesIO
import streamlit_authenticator as stauth
from datetime import datetime, timedelta
import plotly.graph_objects as go
import re
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
    valid_ga4 = ga4.loc[ga4['event_date'].isin(hotmart['order_date'])].copy()
    valid_ga4['utm_source_std'] = valid_ga4.apply(lambda x: 'Direct' if (x['utm_source_std'] == 'Others') & (x['default_channel'] == 'Direct') else x['utm_source_std'], axis=1)
    st.session_state['valid_df'] = valid_ga4

def map_source(source: str) -> str:
    source = source.lower().strip()
    
    # Define regular expression patterns
    active_campaign_pattern = re.compile(r'(?:^|[^a-zA-Z0-9_])(active)(?:$|[^a-zA-Z0-9_])', re.IGNORECASE)
    email_pattern = re.compile(r'email|e-mail|e_mail|e mail', re.IGNORECASE)

    if re.search(r'\b(?:ig|fb)(?:[_]|$)', source, re.IGNORECASE) or 'facebook' in source or 'instagram' in source:
        return 'Facebook + Instagram'
    elif 'youtube' in source:
        return 'YouTube'
    elif 'bing' in source:
        return 'Bing'
    elif 'blog' in source:
        return 'Blog'
    elif 'linkedin' in source or 'lnkd.in' in source:
        return 'Linkedin'
    elif 'tiktok' in source:
        return 'Tik Tok'
    elif 'google' in source:
        return 'Google'
    elif 'activecampaign' in source or active_campaign_pattern.search(source) or email_pattern.search(source):
        return 'Active Campaign'
    elif 'direct' in source:
        return 'Direct'
    elif 'whatsapp' in source or re.search(r'(?:^|[^a-zA-Z0-9_])(wpp|l\.wl\.co)(?:$|[^a-zA-Z0-9_])', source, re.IGNORECASE):
        return 'Whatsapp'
    elif 'hub' in source:
        return 'Hub'
    elif 'hotmart' in source:
        return 'HotMart'
    elif 'yahoo' in source:
        return 'Yahoo'
    else:
        return 'Others'

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
        'transaction_id': lambda x: list(x.unique()),
        'event_date': lambda x: x.min()
    }).reset_index()

    user_journey.columns = ['user_inferred_id', 'utm_source_std', 'default_channel', 'utm_campaign', 'utm_content', 'transaction_id', 'start_date']
    return user_journey

@st.cache_data
def add_sales_team_contributions(ga4: pd.DataFrame, hotmart: pd.DataFrame):
    '''
    Modify the ga4 DataFrame to add the sales team contributions
    '''
    ga = ga4.copy()
    hotmart_filtered_transactions = hotmart.loc[hotmart['tracking.source_sck'].str.contains('venda'), 'transaction']
    ga4_session = ga4.loc[ga4['transaction_id'].isin(hotmart_filtered_transactions), 'ga_session_id']
    valid_ga4 = ga4.loc[ga4['ga_session_id'].isin(ga4_session)].copy()
    valid_ga4['utm_source_std'] = valid_ga4.groupby('ga_session_id', observed=True)['utm_source_std'].transform(lambda x: 'Vendas' if any(x == 'Direct') else x)
    
    # Use boolean indexing to update the correct rows in ga
    ga.loc[ga['ga_session_id'].isin(valid_ga4['ga_session_id']), 'utm_source_std'] = valid_ga4['utm_source_std'].values
    
    return ga

@st.cache_data
def get_hotmart_journey(hotmart_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts the UTMs parameters stored in tracking.source column in hotmart_df
    """
    hotmart = hotmart_df.copy()
    #scr part
    hotmart['utm_source_std'] = hotmart['tracking.source'].apply(lambda x: map_source(x.split('|')[1]) if x is not None and '|' in x and x.split('|')[1].isalnum() else None)
    #sck part
    hotmart['utm_source_std'] = hotmart.apply(lambda x: ['Vendas', x['utm_source_std']] if 'vendas' in x['tracking.source_sck'] else x['utm_source_std'], axis=1)
    hotmart['utm_source_std'] = hotmart.apply(lambda x: ['Active Campaign',x['utm_source_std']] if 'mail' in x['tracking.source_sck'] else x['utm_source_std'], axis=1)
    return hotmart

@st.cache_data
def merge_journeys(journey_ga4: pd.DataFrame, journey_hotmart: pd.DataFrame) -> pd.DataFrame:
    """
    Merges the DataFrames containing the user journey from GA4 and Hotmart info
    """
    # Filter Hotmart DataFrame by approved or complete transactions
    hotmart_filtered = journey_hotmart[journey_hotmart['status'].isin(['APPROVED', 'COMPLETE'])][['transaction', 'utm_source_std']]
    
    # Merge DataFrames using transaction IDs
    merged = pd.merge(journey_ga4.explode('transaction_id'), hotmart_filtered.explode('transaction'), left_on='transaction_id', right_on='transaction', suffixes=('_ga4', '_hotmart'), how='right')
    
    # Convert non-list values in 'utm_source_std' columns to lists and remove None values
    merged['utm_source_std_ga4'] = merged['utm_source_std_ga4'].apply(lambda x: [item for item in x if item is not None] if isinstance(x, list) else [])
    merged['utm_source_std_hotmart'] = merged['utm_source_std_hotmart'].apply(lambda x: [item for item in x if item is not None] if isinstance(x, list) else [])
    
    # Concatenate utm_source_std lists
    merged['utm_source_std'] = merged['utm_source_std_ga4'] + merged['utm_source_std_hotmart']

    for index, row in merged.iterrows():
        if len(row['utm_source_std']) == 0:
            merged.at[index, 'utm_source_std'] = ['Desconhecido']
    # Group by transaction and aggregate utm_source_final lists
    merged_grouped = merged.groupby('transaction').agg({'utm_source_std': 'sum'}).reset_index()
    merged_grouped['utm_source_std'] = merged_grouped['utm_source_std'].apply(lambda x: list(set(x)))
    return merged_grouped

@st.cache_data
def add_revenue_to_journey(merged_journeys_df: pd.DataFrame, hotmart_data: pd.DataFrame) -> pd.DataFrame:
    """
    Adds to the DataFrame user_journey the revenue associated with each user.
    """

    user_journey = merged_journeys_df.copy()
    valid_transactions = hotmart_data[(hotmart_data['status'].isin(['APPROVED', 'COMPLETE'])) & (hotmart_data['source'] == 'PRODUCER')]
    transaction_revenue = valid_transactions.groupby('transaction')['commission.value'].sum().reset_index()
    user_journey_with_revenue = pd.merge(user_journey, transaction_revenue, left_on='transaction', right_on='transaction', how='left')
    user_journey_with_revenue['commission.value'] = user_journey_with_revenue['commission.value'].fillna(0)
    return user_journey_with_revenue

@st.cache_data
def get_revenue_by_source(user_journey_with_revenue: pd.DataFrame) -> pd.DataFrame:
    """
    Returns the revenue by source
    """
    # Divide revenue equally among sources
    user_journey_with_revenue['revenue_per_source'] = user_journey_with_revenue['commission.value'] / user_journey_with_revenue['utm_source_std'].apply(len)
    # Calculate revenue by source
    revenue_by_source = user_journey_with_revenue.explode('utm_source_std').groupby('utm_source_std')['revenue_per_source'].sum().reset_index()
    revenue_by_source = revenue_by_source.rename(columns={'commission.value':'revenue'})
    return revenue_by_source

@st.cache_data
def sanitize_journeys(final_user_journey: pd.DataFrame) -> pd.DataFrame:
    """
    Looks in final_user_journey for journey where Direct isn't the only source in utm_source_std
    and removes it from there. Why am I doing it? If the user has multiple sources and one of them is direct it problably means
    that he came from another source ad save the link for later, or at least he was exposed to the site before
    """
    multi_source_journeys = final_user_journey[final_user_journey['utm_source_std'].apply(lambda x: len(x) > 1 and 'Direct' in x)]

    # Remove Direct from those journeys
    multi_source_journeys['utm_source_std'] = multi_source_journeys['utm_source_std'].apply(lambda x: [source for source in x if source != 'Direct'])

    # Update the final_user_journey DataFrame
    final_user_journey.loc[multi_source_journeys.index, 'utm_source_std'] = multi_source_journeys['utm_source_std']

    return final_user_journey

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
    date_range = st.sidebar.date_input("Periodo atual", value=(valid_ga4['event_date'].max() - timedelta(days=6), valid_ga4['event_date'].max()), max_value=valid_ga4['event_date'].max(), min_value=valid_ga4['event_date'].min(), key='vendas_por_canal_dates')
    limited_ga4 = valid_ga4.loc[(valid_ga4['event_date'].dt.date >= date_range[0]) & (valid_ga4['event_date'].dt.date <= date_range[1])]
    limited_hotmart = hotmart.loc[(hotmart['approved_date'].dt.date >= date_range[0]) & 
                                    (hotmart['approved_date'].dt.date <= date_range[1]) & 
                                    (hotmart['status'].isin(['APPROVED','COMPLETE']))] #desprezando compras canceladas

 

    ################################## BEGIN  #################################
    limited_hotmart = get_hotmart_journey(limited_hotmart)
    limited_ga4 = add_sales_team_contributions(ga4=limited_ga4, hotmart=limited_hotmart)
    user_journey = get_user_journey(limited_ga4)
    merged_journey = merge_journeys(journey_ga4=user_journey, journey_hotmart=limited_hotmart) #Existem transações fantasma no GA4 com id parecido com Hotmart
    final_journey = add_revenue_to_journey(merged_journeys_df=merged_journey, hotmart_data=limited_hotmart)
    final_journey = sanitize_journeys(final_user_journey=final_journey)
    revenue_by_source = get_revenue_by_source(user_journey_with_revenue=final_journey)

    col_1, col_2 = st.columns(2)
    with col_1:     
            st.subheader('% Faturamento Indentificado')
            target = limited_hotmart.loc[(limited_hotmart['status'].isin(['APPROVED','COMPLETE']))
                                        & (limited_hotmart['source'] == 'PRODUCER'), 'commission.value'].sum()
            target_fig = go.Figure()
            target_fig.add_trace(trace=go.Indicator(mode = "gauge+number+delta", value = round(target/revenue_by_source['revenue_per_source'].sum() * 100,0),
                                                    title = {'text': "Faturamento Identificado"}, delta={'reference':100},
                                                    gauge={'shape': 'bullet'}
                                                    ))
            st.plotly_chart(target_fig, use_container_width=True)

    with col_2:
        sources_fig = px.pie(data_frame=revenue_by_source, names='utm_source_std', values='revenue_per_source', title='Distribuição do faturamento')
        st.plotly_chart(sources_fig, use_container_width=True)
