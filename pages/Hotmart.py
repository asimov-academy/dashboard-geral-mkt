import streamlit as st
import streamlit_authenticator as stauth
from FacebookAds import get_data_from_bucket, process_data
from io import BytesIO
from datetime import timedelta
import pandas as pd
from millify import millify
from plotly.subplots import make_subplots
import plotly.express as px
import plotly.graph_objects as go

def get_metrics(df: pd.DataFrame, fb_data: pd.DataFrame, date_range:list) -> dict:
    """
    Calculates the metrics (add metrics here) for a given df
    """
    metrics = dict()
    valid_df = df.loc[df['status'].isin(['APPROVED', 'COMPLETE'])]
    metrics['billing'] = valid_df.loc[valid_df['source'] == 'PRODUCER', 'commission.value'].sum()
    metrics['n_valid_sales'] = valid_df.loc[(valid_df['source'] == 'PRODUCER'), 'transaction'].nunique()
    metrics['refunds'] = len(df.loc[(df['status'] == 'REFUNDED') & (df['approved_date'].dt.date >= date_range[0]) & (df['approved_date'].dt.date <= date_range[1]), 'transaction'].unique())
    metrics['avarage_ticket'] = metrics['billing'] / metrics['n_valid_sales']
    metrics['affiliates_sales'] = len(valid_df.loc[(valid_df['source'] == 'AFFILIATE'), 'transaction'])
    transactions_by_affiliates = valid_df.loc[valid_df['source'] == 'AFFILIATE', 'transaction']
    metrics['affiliates_revenue'] = valid_df.loc[(valid_df['transaction'].isin(transactions_by_affiliates)) & (valid_df['source'] == 'PRODUCER'), 'commission.value'].sum()
    metrics['sales_team_sales'] = len(valid_df.loc[valid_df['tracking.source_sck'].str.split('_').apply(lambda x: x[0]).str.contains('venda'), 'transaction'])
    metrics['sales_team_revenue'] = valid_df.loc[valid_df['tracking.source_sck'].str.split('_').apply(lambda x: x[0]).str.contains('venda'), 'commission.value'].sum()
    metrics['profit'] = metrics['billing'] - fb_data['spend'].sum()
    if ((valid_df['tracking.source_sck'].str.contains(pat='email')) | (valid_df['tracking.source'].str.contains(pat='email'))).sum() > 0:
        metrics['email_revenue'] = valid_df.loc[((valid_df['tracking.source_sck'].str.split('_').apply(lambda x: x[0]).str.contains(pat='email'))|
                                                (valid_df['tracking.source'].str.contains(pat='email'))) &
                                                (valid_df['source'] == 'PRODUCER'), 'commission.value'].sum()
    else:
        metrics['email_revenue'] = 0
    return metrics


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
    st.title('Dados Hotmart')

    try:
        hotmart = st.session_state['hotmart_data']
    except:
        tmp_hotmart = get_data_from_bucket(bucket_name='dashboard_marketing_processed', file_name='processed_hotmart.parquet', file_type='.parquet')
        raw_hotmart = pd.read_parquet(BytesIO(tmp_hotmart), engine='pyarrow')
        raw_hotmart['count'] = 1
        raw_hotmart['order_date'] = pd.to_datetime(raw_hotmart['order_date'])
        raw_hotmart['tracking.source_sck'] = raw_hotmart['tracking.source_sck'].fillna(value='Desconhecido')
        raw_hotmart['tracking.source'] = raw_hotmart['tracking.source'].fillna(value='Desconhecido')
        raw_hotmart['approved_date'] = pd.to_datetime(raw_hotmart['approved_date'])
        st.session_state['hotmart_data'] = raw_hotmart
        hotmart = st.session_state['hotmart_data'] = raw_hotmart
    try:
        fb = st.session_state['fb']
    except:
        fb = process_data('processed_adsets.csv')
        st.session_state['fb'] = fb
    

    ############# FILTRANDO OS DADOS ###########################################
    
    date_range = st.sidebar.date_input("Periodo atual", value=(pd.to_datetime(hotmart['order_date']).max()-timedelta(days=6), pd.to_datetime(hotmart['order_date']).max()), max_value=pd.to_datetime(hotmart['order_date']).max(), min_value=pd.to_datetime(hotmart['order_date']).min(), key='hotmart_dates')
    dates_benchmark_hotmart = st.date_input("Periodo de para comparação", value=(pd.to_datetime(hotmart['order_date']).max()-timedelta(days=13), pd.to_datetime(hotmart['order_date']).max()-timedelta(days=7)), max_value=pd.to_datetime(hotmart['order_date']).max(), min_value=pd.to_datetime(hotmart['order_date']).min(), key='hotmart_dates_benchmark')
    limited_hotmart = hotmart.loc[(hotmart['order_date'].dt.date >= date_range[0]) & 
                                  (hotmart['order_date'].dt.date <= date_range[1]) & 
                                  (hotmart['status'].isin(['APPROVED','REFUNDED','COMPLETE']))] #desprezando compras canceladas
    
    benchmark = hotmart.loc[(hotmart['order_date'].dt.date >= dates_benchmark_hotmart[0]) & 
                            (hotmart['order_date'].dt.date <= dates_benchmark_hotmart[1]) & 
                            (hotmart['status'].isin(['APPROVED','REFUNDED','COMPLETE']))] #desprezando compras canceladas

    limited_fb = fb.loc[(fb['date'] >= date_range[0]) & (fb['date'] <= date_range[1])]
    benchmark_fb = fb.loc[(fb['date'] >= dates_benchmark_hotmart[0]) & (fb['date'] <= dates_benchmark_hotmart[1])]
    ################ CALCULOS #######################################################
    current_metrics = get_metrics(limited_hotmart, limited_fb, date_range)
    benchmark_metrics = get_metrics(benchmark, benchmark_fb,dates_benchmark_hotmart)
    options = {'Faturamento' : 'commission.value',
               'Vendas' : 'count'}
    ################ INICIO #########################################################   

    col_1, col_2, col_3 = st.columns(3)
    with col_1:
        st.metric('Faturamento', value = f'R$ {millify(current_metrics["billing"], precision=1)}', delta = millify(current_metrics['billing'] - benchmark_metrics['billing'], precision=1))
        st.metric('Gasto na campanha de conversão (Facebook)', value=f'R$ {millify(limited_fb["spend"].sum(), precision=1)}', delta=millify(limited_fb['spend'].sum() - benchmark_fb['spend'].sum(), precision=1), delta_color='off')
        st.metric('Ticket Médio', value=f'R$ {millify(current_metrics["avarage_ticket"], precision=1)}', delta=millify(current_metrics['avarage_ticket'] - benchmark_metrics['avarage_ticket'], precision=1))
    
    with col_2:
        st.metric('Lucro aproximado', value=f'R${millify(current_metrics["profit"], precision=1)}', delta=millify(current_metrics['profit'] - benchmark_metrics['profit'], precision=1))
        st.metric('ROAS aproximado', value=round(current_metrics['billing']/limited_fb['spend'].sum(),2), delta=round((current_metrics['billing']/limited_fb['spend'].sum() - (benchmark_metrics['billing']/benchmark_fb['spend'].sum())),2))
        st.metric('Faturamento - Time de vendas', value=f'R$ {millify(current_metrics["sales_team_revenue"], precision=1)}', delta=millify(current_metrics["sales_team_revenue"] - benchmark_metrics["sales_team_revenue"], precision=1))
        st.metric('Faturamento - Afiliados', value=f'R$ {millify(current_metrics["affiliates_revenue"], precision=1)}', delta=millify(current_metrics["affiliates_revenue"] - benchmark_metrics["affiliates_revenue"], precision=1))
        st.metric('Faturamento - e-mail marketing', value=f'R$ {millify(current_metrics["email_revenue"], precision=1)}', delta=millify(current_metrics["email_revenue"] - benchmark_metrics["email_revenue"], precision=1))

    with col_3:
        st.metric('Vendas', value=current_metrics['n_valid_sales'], delta=current_metrics['n_valid_sales'] - benchmark_metrics['n_valid_sales'])
        st.metric('Reembolsos', value=current_metrics['refunds'], delta= current_metrics['refunds'] - benchmark_metrics['refunds'], delta_color='inverse')
        st.metric('Time de vendas', value=current_metrics['sales_team_sales'], delta=current_metrics['sales_team_sales'] - benchmark_metrics['sales_team_sales'])
        st.metric('Afiliados', value=current_metrics['affiliates_sales'], delta=current_metrics['affiliates_sales'] - benchmark_metrics['affiliates_sales'])

    ################## PLOT SCk ######################################
    sck_figure = px.pie(data_frame=limited_hotmart.loc[(limited_hotmart['status'] != 'REFUNDED') & (limited_hotmart['source'] == 'PRODUCER')], values='count', names= 'tracking.source_sck', hole=0.5, 
                        title='Distribuição das vendas por sck', height=600).update_traces(textinfo='percent+value')
    st.plotly_chart(sck_figure, use_container_width=True)

    ###################### PLOT PRODUCTS #############################
    product_revenue = limited_hotmart.loc[(limited_hotmart['status'] != 'REFUNDED') & (limited_hotmart['source'] == 'PRODUCER'), ['commission.value', 'product_name']].groupby(by='product_name').sum()
    n_products = limited_hotmart.loc[(limited_hotmart['status'] != 'REFUNDED') & (limited_hotmart['source'] == 'PRODUCER'), ['count', 'product_name']].groupby(by='product_name').sum()
    product_figure = make_subplots(rows=1, cols=2, column_titles=['Distribuição dos items vendidos', 'Faturamento por item'], shared_yaxes=True, specs=[[{"type": "pie"}, {"type": "pie"}]])
    
    product_figure.add_trace(trace= go.Pie(labels=n_products.index, values=n_products['count'], domain=dict(x=[0, 0.5])), row=1, col=1).update_traces(textinfo='percent+value')
    product_figure.add_trace(go.Pie(labels=product_revenue.index, values=product_revenue['commission.value'],domain=dict(x=[0.51, 1.0])), row=1, col=2).update_traces(textinfo='percent+value')
    st.plotly_chart(product_figure, use_container_width=True)

    hotmart_metric = st.selectbox(label='Selecione uma métrica para acompanhar a evolução', options=['Faturamento', 'Vendas'], index=1)
    if hotmart_metric == 'Faturamento':
        historic_data = hotmart.loc[hotmart['status'].isin(['APPROVED', 'COMPLETE']) & (hotmart['source'] == 'PRODUCER'), ['approved_date', 'commission.value', 'count']].groupby(by='approved_date').sum()
    else:
        historic_data = hotmart.loc[hotmart['status'].isin(['APPROVED', 'COMPLETE']), ['approved_date', 'commission.value', 'count']].groupby(by='approved_date').sum()
    
    historic_data.sort_index(ascending=True, inplace=True)
    historic_fig = px.line(data_frame=historic_data, x=historic_data.index, y=options[hotmart_metric], title=f'Histórico da metrica: {hotmart_metric}')
    st.plotly_chart(historic_fig, use_container_width=True)


    

    
    
