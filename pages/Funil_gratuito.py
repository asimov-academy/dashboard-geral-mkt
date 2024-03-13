import streamlit as st
import gspread
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px
from millify import millify

from FacebookAds import get_data_from_bucket

######################## Getting the data ############################
sheets_key = st.secrets['GOOGLE_SHEETS']

try:
    sheets_data = st.session_state['google_sheets']

except:
    gc = gspread.service_account_from_dict(sheets_key)
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1S8obXt7hmaiab_qIz73yQBDvQoFwuHYWNvEvtuhikys/edit#gid=0')
    w_sheet = sh.get_worksheet(0)
    tmp = pd.DataFrame(w_sheet.get_all_records())
    tmp['Data'] = pd.to_datetime(tmp['Data'])
    st.session_state['google_sheets'] = tmp
    sheets_data = st.session_state['google_sheets']

try:
    hotmart = st.session_state['hotmart_data']

except:
    tmp_hotmart = get_data_from_bucket(bucket_name='dashboard_marketing_processed', file_name='processed_hotmart.parquet', file_type='.parquet')
    raw_hotmart = pd.read_parquet(BytesIO(tmp_hotmart), engine='pyarrow')
    raw_hotmart['count'] = 1
    raw_hotmart['order_date'] = pd.to_datetime(raw_hotmart['order_date'])
    raw_hotmart['approved_date'] = pd.to_datetime(raw_hotmart['approved_date'])
    raw_hotmart['tracking.source_sck'] = raw_hotmart['tracking.source_sck'].fillna(value='Desconhecido')
    raw_hotmart['tracking.source'] = raw_hotmart['tracking.source'].fillna(value='Desconhecido')
    st.session_state['hotmart_data'] = raw_hotmart
    hotmart = st.session_state['hotmart_data'] = raw_hotmart

try:
    funnel_data = st.session_state['sheets_hot_merged']
except:
    st.session_state['sheets_hot_merged'] = sheets_data.merge(hotmart[['email', 'approved_date', 'status', 'tracking.source', 'tracking.source_sck', 'source', 'commission.value']], left_on='Email', right_on='email', how='left')
    tmp = st.session_state['sheets_hot_merged']
    tmp['conversion_time'] = pd.to_datetime(tmp['approved_date']) - tmp['Data']
    tmp['conversion_time'] = tmp['conversion_time'].dt.days
    funnel_data = tmp.loc[tmp['conversion_time'] >= 0].copy()
#########################################################################
def get_funnel_metrics(df):

    metrics = {}
    metrics['revenue'] = df.loc[(df['status'].isin(['COMPLETE', 'APPROVED']))
                                &(df['source'] == 'PRODUCER')
                                &(df['tracking.source_sck'].isin(['basico', 'basico-expirou','seja-pro'])), 'commission.value'].sum()
    metrics['n_sales'] = df.loc[(~df['commission.value'].isna())
                                &(df['status'].isin(['COMPLETE', 'APPROVED']))
                                &(df['source'] == 'PRODUCER')
                                &(df['tracking.source_sck'].isin(['basico', 'basico-expirou','seja-pro']))].shape[0]
    
    metrics['conversion_rate'] = metrics['n_sales'] / len(df['Email'].unique()) * 100
    
    metrics['average_conversion_time'] = df.loc[(df['status'].isin(['COMPLETE', 'APPROVED']))
                                                & (~df['conversion_time'].isna())
                                                &(df['source'] == 'PRODUCER'), 'conversion_time'].mean()
    return metrics

#################### FILTER DATA ########################################
date_range = st.sidebar.date_input(label="Periodo atual", value=(funnel_data['Data'].max()-timedelta(days=6), funnel_data['Data'].max() - timedelta(days=1)), max_value=funnel_data['Data'].max()- timedelta(days=1), min_value=funnel_data['Data'].min(), key='funnel_dates')
dates_range_benchmark = st.date_input(label="Periodo de para comparação", value=[funnel_data['Data'].max()-timedelta(days=14), funnel_data['Data'].max() - timedelta(days=7)], max_value=funnel_data['Data'].max() - timedelta(days=1), min_value=funnel_data['Data'].min(), key='funnel_dates_benchmark')

limited_funnel = funnel_data.loc[(funnel_data['Data'].dt.date >= date_range[0])
                                 & (funnel_data['Data'].dt.date <= date_range[1])]
benchmark_funnel = funnel_data.loc[(funnel_data['Data'].dt.date >= dates_range_benchmark[0])
                                   & (funnel_data['Data'].dt.date >= dates_range_benchmark[1])]

current_funnel_metrics = get_funnel_metrics(limited_funnel)
benchmark_funnel_metrics = get_funnel_metrics(benchmark_funnel)
########################## PRE-SETS ######################################

####################### BEGINIG OF DASH ##################################
st.title('Funil Gratuito - Visão Geral')

col_1, col_2 = st.columns(2)
with col_1:
    target = st.number_input('Qual o valor da meta para o período', value=50000)
    target_fig = go.Figure()
    target_fig.add_trace(trace=go.Indicator(mode = "gauge+number", value = round(number=current_funnel_metrics['revenue'] / target * 100), title = {'text': " % Meta"},
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
    inner_col1, inner_col2, inner_col3 = st.columns(3)
    
    with inner_col1:
        st.metric(label='Faturamento', value=f'R$ {millify(current_funnel_metrics["revenue"], precision=1)}', delta=millify(current_funnel_metrics['revenue'] - benchmark_funnel_metrics['revenue'], precision=1))
        st.metric(label='Investimento', value=f'R$ {0.00}')
    
    with inner_col2:
        st.metric(label='Vendas', value=current_funnel_metrics['n_sales'], delta=current_funnel_metrics['n_sales'] - benchmark_funnel_metrics['n_sales'])
        st.metric(label='Leads - Totais', value= funnel_data['Email'].nunique())
        st.metric(label='Janela média de conversão', value=round(current_funnel_metrics['average_conversion_time'],0), delta=round(current_funnel_metrics['average_conversion_time'] - benchmark_funnel_metrics['average_conversion_time'],0), delta_color='inverse')

    with inner_col3:
        st.metric(label='Taxa de conversão', value=f'{round(current_funnel_metrics["conversion_rate"],1)}%', delta=round(current_funnel_metrics['conversion_rate'] - benchmark_funnel_metrics['conversion_rate'], 1))
        st.metric(label='CPL', value=f'R$ {0.0}')
        

col_3, col_4 = st.columns(2)
with col_3:
    sck_fig = px.pie(data_frame=limited_funnel.loc[(~limited_funnel['commission.value'].isna())
                                &(limited_funnel['status'].isin(['COMPLETE', 'APPROVED']))
                                &(limited_funnel['source'] == 'PRODUCER')
                                &(limited_funnel['tracking.source_sck'].isin(['basico', 'basico-expirou','seja-pro']))], names='tracking.source_sck', values='commission.value', title='Vendas por SCK')
    st.plotly_chart(sck_fig)

with col_4:
    scr_fig = px.pie(data_frame=limited_funnel, names='tracking.source', values='commission.value', title='Vendas por SRC')
    st.plotly_chart(scr_fig, use_container_width=True)

st.subheader('Evolução histórica')
g_data = funnel_data[['Data', 'Email', 'approved_date']].groupby(by='Data').count().reset_index()

hist_dates = st.date_input(label='Selecione o periodo desejado', value=[funnel_data['Data'].min(), funnel_data['Data'].max() - timedelta(days=1)], max_value=funnel_data['Data'].max() - timedelta(days=1), min_value=funnel_data['Data'].min())
g_data = g_data.loc[(g_data['Data'].dt.date >= hist_dates[0])
                    & (g_data['Data'].dt.date <= hist_dates[1])]
hist_fig = go.Figure()
hist_fig.add_trace(trace=go.Scatter(x=g_data['Data'], y=g_data['Email'], name='Leads'))
hist_fig.add_trace(trace=go.Scatter(x=g_data['Data'], y=g_data['approved_date'], name='Compras'))
hist_fig.update_layout(title='Leads vs Compras', showlegend=True)
st.plotly_chart(hist_fig, use_container_width=True)