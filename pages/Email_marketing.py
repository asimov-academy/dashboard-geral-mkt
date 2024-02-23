import streamlit as st
import streamlit_authenticator as stauth
from FacebokAds import get_data_from_bucket
from io import BytesIO
from datetime import timedelta, datetime
import pandas as pd
from millify import millify
from plotly.subplots import make_subplots
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from math import ceil

@st.cache_data
def get_active_metrics(data: pd.DataFrame) -> dict:
      """
      Computes the metrics: n_mails_send, n_unique_clicks, n_opens,
        n_replies, n_bounces, n_unsub.
      
      Parameters
      
      data : pd.DataFrame

      Returns:
      metrics : dict

      """
      metrics = dict()
      grouped_data = data[['automation_name', 'headline', 'send_amt', 'uniquelinkclicks', 'uniqueopens','replies','hardbounces', 'unsubscribes']].groupby(by=['automation_name', 'headline']).sum()
      grouped_data['ctr'] = grouped_data['uniquelinkclicks']/grouped_data['send_amt']
      grouped_data['open_rate'] = grouped_data['uniqueopens']/grouped_data['send_amt']
      metrics['open_rate'] = grouped_data['open_rate'].mean()
      metrics['ctr'] = grouped_data['ctr'].mean()
      tmp_auto = data['automation_name'].unique()
      metrics['n_automations'] = len(tmp_auto[tmp_auto != "Sem automação"])
      metrics['n_mails_send'] = data['send_amt'].sum()
      metrics['n_unique_clicks'] = grouped_data['uniquelinkclicks'].mean()
      metrics['n_opens'] = grouped_data['uniqueopens'].mean()
      metrics['n_replies'] = grouped_data['replies'].mean()
      metrics['n_bounces'] = grouped_data['hardbounces'].mean()
      metrics['n_unsub'] = grouped_data['unsubscribes'].mean()
      return metrics


def get_n_email_sessions(ga4: pd.DataFrame) -> int:
     return len(ga4.loc[(ga4['utm_source_std'] == 'Active Campaign')
                        & (ga4['event_name'] == 'session_start')])

#TODO
def get_upgrades(hotmart_df: pd.DataFrame, active_contacts_df: pd.DataFrame, active_tags: pd.DataFrame) -> int:
    """
    Given the hotmart transaction info on hotmart_df looks for the transactions that are valid (APROVED or COMPLETE) checks the
    tracking.source_sck if it is equal to email-upgrade count it in the n_upgrades. Besides that it also get the buyer's e-mail extract the 
    buyers_id via active_contacts_df and finnaly with the buyers_id checks in active_contacts_df if the buyer has the TAGs [to be defined]
    """
    return 0 


def get_email_revenue_sales(hotmart: pd.DataFrame) -> dict:
    """
    Get the e-mail revenue and n_sales from Hotmart DataFrame, by filtering only valid transactions (APPROVED or COMPLETED)
    whose tracking.sck contains "email".

    PARAMETERS:

    ga4 : pd.DataFrame

    RETURNS
    dict 
    """

    valid_df = hotmart.loc[hotmart['status'].isin(['APPROVED', 'COMPLETE'])]
    hotmart_mail = {}

    if ((valid_df['tracking.source_sck'].str.contains(pat='email')) | valid_df['tracking.source'].str.contains(pat='email')).sum() > 0:
        hotmart_mail['email_revenue'] = valid_df.loc[((valid_df['tracking.source_sck'].str.contains(pat='email')) | (valid_df['tracking.source'].str.contains(pat='email')))
                                                     & (valid_df['source'] == 'PRODUCER'), 'commission.value'].sum()
    else:
        hotmart_mail['email_revenue'] = 0
    
    hotmart_mail['email_sales'] = len(valid_df.loc[(valid_df['tracking.source_sck'].str.contains(pat='email')) |(valid_df['tracking.source'].str.contains('email')), 'transaction'].unique())
    hotmart_mail['cart_abandonment'] = valid_df.loc[valid_df['tracking.source_sck'].str.contains('email-abandono-carrinho'), 'transaction'].nunique()

    return hotmart_mail

def get_new_leads(active_contacts_df: pd.DataFrame, forbidden_tags: list) -> int:
    """
    Get the number of new leads based on cdate in active_contacts_df and if these contacts doesn't have the forbidden TAGs 
    """
    filtered_contacts = active_contacts_df.loc[~active_contacts_df['tag'].apply(lambda x: isinstance(x, np.ndarray) and any(tag in x for tag in forbidden_tags)), 'id'].nunique()
    return filtered_contacts 



try:
    active_campaign = st.session_state['active_campaign']
except:
    tmp_active = get_data_from_bucket(bucket_name='dashboard_marketing_processed', file_name='ActiveCampaign.feather', file_type='.feather')
    raw_active = pd.read_feather(BytesIO(tmp_active))
    raw_active['automation_name'] = raw_active['automation_name'].fillna(value='Sem automação')
    st.session_state['active_campaign'] = raw_active
    active_campaign = st.session_state['active_campaign']

try:
    active_contacts = st.session_state['active_campaign_contacts']
except:
    tmp_contacts = get_data_from_bucket(bucket_name='dashboard_marketing_processed', file_name='contacts_activecampaign.feather', file_type='.feather')
    raw_contacts = pd.read_feather(BytesIO(tmp_contacts))
    raw_contacts['id'] = raw_contacts['id'].astype(int)
    
    tmp_tag = get_data_from_bucket(bucket_name='dashboard_marketing_processed', file_name='ActiveCampaign_contacts_TAGs.feather', file_type='.feather')
    active_tags = pd.read_feather(BytesIO(tmp_tag))
    active_tags['contact'] = active_tags['contact'].astype(int)
    active_tags['tag'] = active_tags['tag'].apply(lambda x: x.astype(int))
    active_contacts = raw_contacts.merge(active_tags, left_on='id', right_on='contact', how='left')
    active_contacts.drop(['contact'], axis=1, inplace=True)
    st.session_state['active_campaign_contacts'] = active_contacts

try:
    ga4 = st.session_state['ga4']
except:
    tmp_ga4 = get_data_from_bucket(bucket_name='dashboard_marketing_processed', file_name='ga4_data_dash.parquet', file_type='.parquet')
    raw_ga4 = pd.read_parquet(BytesIO(tmp_ga4), engine='pyarrow')
    st.session_state['ga4'] = raw_ga4
    ga4 = st.session_state['ga4']

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



####################### FILTRANDO OS DADOS  #####################################
date_range = st.sidebar.date_input("Periodo atual", value=(active_campaign['last_date'].max()-timedelta(days=6), active_campaign['last_date'].max()), max_value=active_campaign['last_date'].max(), min_value=active_campaign['last_date'].min(), key='active_dates')
dates_benchmark_active = st.date_input("Periodo de para comparação", value=(active_campaign['last_date'].max()-timedelta(days=13), active_campaign['last_date'].max()-timedelta(days=7)), max_value=active_campaign['last_date'].max(), min_value=active_campaign['send_date'].min(), key='active_dates_benchmark')

limited_active = active_campaign.loc[(active_campaign['last_date'].dt.date >= date_range[0]) & (active_campaign['last_date'].dt.date <= date_range[1])]
limited_active_benchmark = active_campaign.loc[(active_campaign['last_date'].dt.date >= dates_benchmark_active[0]) & (active_campaign['last_date'].dt.date <= dates_benchmark_active[1])]

limited_contacts = active_contacts.loc[(active_contacts['cdate'].dt.date >= date_range[0]) & (active_contacts['cdate'].dt.date <= date_range[1])]
limited_contacts_benchmark = active_contacts.loc[(active_contacts['cdate'].dt.date >= dates_benchmark_active[0]) & (active_contacts['cdate'].dt.date <= dates_benchmark_active[1])]

############## HARDCODED PRE-SETS #################################################
forbidden_tags = [172,214,246,252,258,264,270,276]

##### OUTRAS FONTES #####
limited_ga4 = ga4.loc[(ga4['event_date'].dt.date >= date_range[0]) & 
                      (ga4['event_date'].dt.date <= date_range[1]) &
                      (ga4['event_name'] == 'session_start')]

limited_ga4_benchmark = ga4.loc[(ga4['event_date'].dt.date >= dates_benchmark_active[0]) &
                            (ga4['event_date'].dt.date <= dates_benchmark_active[1]) &
                            (ga4['event_name'] == 'session_start')]

if ((len(limited_ga4) == 0) | (len(limited_active_benchmark) == 0)):
     st.warning(f'"🚨" dados do GA4 indisponíveis para o periodo selecionado período disponível {ga4["event_date"].max()} - {ga4["event_date"].min()}')

limited_hotmart = hotmart.loc[(hotmart['approved_date'].dt.date >= date_range[0]) & 
                                (hotmart['approved_date'].dt.date <= date_range[1]) & 
                                (hotmart['status'].isin(['APPROVED','REFUNDED','COMPLETE']))] #desprezando compras canceladas

limited_hotmart_benchmark = hotmart.loc[(hotmart['approved_date'].dt.date >= dates_benchmark_active[0]) & 
                        (hotmart['approved_date'].dt.date <= dates_benchmark_active[1]) & 
                        (hotmart['status'].isin(['APPROVED','REFUNDED','COMPLETE']))] #desprezando compras canceladas
if ((len(limited_hotmart) == 0) | (len(limited_hotmart_benchmark) == 0)):
     st.warning(f'"🚨" dados da Hotmart indisponíveis para o periodo selecionado período disponível {hotmart["order_date"].max()} - {hotmart["order_date"].min()}')

###################### BEGIN #####################
st.title('Email Marketing')
current_hotmart = get_email_revenue_sales(limited_hotmart)
benchmark_hot = get_email_revenue_sales(limited_hotmart_benchmark)
current_email_sessions = get_n_email_sessions((limited_ga4))
benchmark_email_sessions = get_n_email_sessions(limited_ga4_benchmark)
current_active_metrics = get_new_leads(active_contacts_df=limited_contacts, forbidden_tags=forbidden_tags)
benchmark_active_metrics = get_new_leads(active_contacts_df=limited_contacts_benchmark, forbidden_tags=forbidden_tags)
email_marketing_target_value = 50000
##################### OVERVIEW ####################
overview = st.expander(label='Visão Geral')

with overview:
    col_1, col_2, col_3, col_4 = st.columns(4)

    with col_1:
        target = st.number_input('Qual o valor da meta para o período', value=50000)
        target_fig = go.Figure()
        target_fig.add_trace(trace=go.Indicator(mode = "gauge+number", value = round(number=current_hotmart['email_revenue'] / target * 100), title = {'text': " % Meta"},
                                                delta={'reference':target},
                                                gauge={
                                                    'axis': {'range': [0, 100]},
                                                    'bar': {'color': 'grey'},
                                                    'steps' : [
                                                        {'range': [0, 20], 'color': 'red'},
                                                        {'range': [20, 40], 'color': 'orange'},
                                                        {'range': [40, 60], 'color': 'yellow'},
                                                        {'range': [60, 80], 'color': 'rgb(144, 238, 144)'},
                                                        {'range': [80, 100], 'color': 'green'}
                                                    ]}
                                                ))

        st.plotly_chart(target_fig, use_container_width=True)
    with col_2:
        st.metric(label='Faturamento', value=f'R$ {millify(current_hotmart["email_revenue"], precision=1)}', delta=millify((current_hotmart['email_revenue'] - benchmark_hot['email_revenue']), precision=1))
        st.metric(label='Total de leads', value=active_contacts['id'].nunique())
    with col_3:
        st.metric(label='Novos clientes', value=current_hotmart['email_sales'], delta=current_hotmart['email_sales'] - benchmark_hot['email_sales'])
        st.metric(label='Novos Leads', value=current_active_metrics, delta=current_active_metrics - benchmark_active_metrics)
    
    with col_4:
        st.metric(label='Abandono de carrinho', value=current_hotmart['cart_abandonment'], delta=current_hotmart['cart_abandonment'] - benchmark_hot['cart_abandonment'])
        st.metric(label='Acessos ao site via e-mail', value=current_email_sessions, delta=current_email_sessions - benchmark_email_sessions)
email_hist_exp = st.expander('Evolução histórica do email marketing')

with email_hist_exp:
    option = st.radio(label='Mudar o período de visualização (pré-set mês corrente)', options=['Sim', 'Não'], horizontal=True, index=1)
    hist_col1, hist_col2 = st.columns(2)
    with hist_col1:
        if option == 'Não':
            month = datetime.today().month       
            hist_sales = hotmart.loc[(hotmart['status'].isin(['APPROVED', 'COMPLETE']))
                        & (hotmart['approved_date'].dt.month == month)
                        & (hotmart['approved_date'].dt.year == datetime.today().year)
                        & ((hotmart['tracking.source_sck'].str.contains(pat='email') | (hotmart['tracking.source'].str.contains(pat='email')))), 
                        ['approved_date', 'transaction']].copy()
            hist_sales['date'] = hist_sales['approved_date'].dt.date
            hist_sales = hist_sales[['date', 'transaction']].groupby(by='date').count().reset_index()
            

            tmp_contacts = active_contacts[['cdate','id', 'tag']].copy()
            tmp_contacts = tmp_contacts.loc[(tmp_contacts['cdate'].dt.month == month)
                                            & (tmp_contacts['cdate'].dt.year == datetime.today().year)
                                            & (~tmp_contacts['tag'].isin(forbidden_tags))]
            tmp_contacts['date'] = tmp_contacts['cdate'].dt.date
            hist_leads = tmp_contacts[['date', 'id']].groupby(by='date').count().reset_index()

            hist_email_sessions = ga4.loc[(ga4['event_name'] == 'session_start') 
                                        & (ga4['utm_source_std'] == 'Active Campaign')
                                        & (ga4['event_date'].dt.month == month)
                                        & (ga4['event_date'].dt.year == datetime.today().year), ['event_date','event_name']].copy()
            hist_email_sessions['date'] = hist_email_sessions['event_date'].dt.date
            hist_email_sessions = hist_email_sessions[['date', 'event_name']].groupby(by='date').count().reset_index()
        

        else:
            hist_dates = st.date_input(label='Selecione o periodo desejado', value=[active_campaign['last_date'].max()-timedelta(days=6), active_campaign['last_date'].max()], max_value=active_campaign['last_date'].max(), min_value=active_campaign['last_date'].min())
            hist_sales = hotmart.loc[(hotmart['status'].isin(['APPROVED', 'COMPLETE']))
            & (hotmart['approved_date'].dt.date >= hist_dates[0])
            & (hotmart['approved_date'].dt.date <= hist_dates[1])
            & ((hotmart['tracking.source_sck'].str.contains(pat='email') | (hotmart['tracking.source'].str.contains(pat='email')))), 
            ['approved_date', 'transaction']].copy()
            hist_sales['date'] = hist_sales['approved_date'].dt.date
            hist_sales = hist_sales[['date', 'transaction']].groupby(by='date').count().reset_index()

            tmp_contacts = active_contacts[['cdate','id', 'tag']].copy()
            tmp_contacts = tmp_contacts.loc[(tmp_contacts['cdate'].dt.date >= hist_dates[0])
                                            & (tmp_contacts['cdate'].dt.date <= hist_dates[1])
                                            & (~tmp_contacts['tag'].isin(forbidden_tags))]
            tmp_contacts['date'] = tmp_contacts['cdate'].dt.date
            hist_leads = tmp_contacts[['date', 'id']].groupby(by='date').count().reset_index()

            hist_email_sessions = ga4.loc[(ga4['event_name'] == 'session_start')
                                          & (ga4['utm_source_std'] == 'Active Campaign')
                                          & (ga4['event_date'].dt.date >= hist_dates[0])
                                          & (ga4['event_date'].dt.date <= hist_dates[1]), ['event_date','event_name']].copy()
            hist_email_sessions['date'] = hist_email_sessions['event_date'].dt.date
            hist_email_sessions = hist_email_sessions[['date', 'event_name']].groupby(by='date').count().reset_index()
        
        
        
        fig_hist_sales = px.line(data_frame=hist_sales, x='date', y='transaction', title='Histórico de vendas de email marketing', markers=True).update_traces(marker_size=10).update_layout(yaxis_range=[0, hist_sales['transaction'].max() + 5], yaxis_title='Número de vendas', xaxis_title='Data')
        st.plotly_chart(fig_hist_sales, use_container_width=True)   

        fig_hist_leads = px.line(data_frame=hist_leads, x='date', y='id', title='Histórico de novos leads').update_layout(yaxis_range=[0, hist_leads['id'].max() + 50], yaxis_title='Número de novos leads', xaxis_title='Data')
        st.plotly_chart(fig_hist_leads, use_container_width=True)

        fig_hist_sessions = px.line(data_frame=hist_email_sessions, x='date', y='event_name', title='Histórico de novas sessões oriundas do email').update_layout(yaxis_range=[0, hist_email_sessions['event_name'].max() + 50], yaxis_title='Número de novas sessões', xaxis_title='Data')
        st.plotly_chart(fig_hist_sessions, use_container_width=True)

    with hist_col2:
        year = datetime.today().year
        hist_sales_y = hotmart.loc[(hotmart['status'].isin(['APPROVED', 'COMPLETE']))
                                 & (hotmart['approved_date'].dt.year == year)
                                 & ((hotmart['tracking.source_sck'].str.contains(pat='email') | (hotmart['tracking.source'].str.contains(pat='email')))), 
                                ['approved_date', 'transaction']].copy()
        
        hist_sales_y['month'] = hist_sales_y['approved_date'].dt.month_name()
        hist_sales_y = hist_sales_y[['month', 'transaction']].groupby(by='month').count().reset_index()
        
        tmp_contacts = active_contacts[['cdate','id', 'tag']].copy()
        tmp_contacts = tmp_contacts.loc[(tmp_contacts['cdate'].dt.year == datetime.today().year)
                                        & (~tmp_contacts['tag'].isin(forbidden_tags))]
        tmp_contacts['month'] = tmp_contacts['cdate'].dt.month_name()
        hist_leads_y = tmp_contacts[['month', 'id']].groupby(by='month').count().reset_index()

        hist_email_sessions = ga4.loc[(ga4['event_name'] == 'session_start')
                                      & (ga4['utm_source_std'] == 'Active Campaign')
                                      & (ga4['event_date'].dt.year == datetime.today().year), ['event_date','event_name']].copy()
    
        hist_email_sessions['month'] = hist_email_sessions['event_date'].dt.month_name()
        hist_email_sessions_y = hist_email_sessions[['month', 'event_name']].groupby(by='month').count().reset_index()   

        fig_hist_sales_y = px.bar(data_frame=hist_sales_y, y='month', x='transaction', title='Cumulativo mensal de vendas de email marketing', text='transaction')#.update_layout(yaxis_range=[0, hist_sales['transaction'].max() + 5], yaxis_title='Número de vendas', xaxis_title='Data')
        st.plotly_chart(fig_hist_sales_y, use_container_width=True)   

        fig_hist_leads = px.bar(data_frame=hist_leads_y, x='id', y='month', title='Cumulativo mensal de novos leads', text='id')
        st.plotly_chart(fig_hist_leads, use_container_width=True)

        fig_hist_sessions = px.bar(data_frame=hist_email_sessions_y, y='month', x='event_name', title='Cumulativo mensal de novas sessões oriundas do email', text='event_name')
        st.plotly_chart(fig_hist_sessions, use_container_width=True)