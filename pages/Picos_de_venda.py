import pandas as pd
import streamlit as st
from FacebookAds import get_data_from_bucket, process_data
from io import BytesIO
from datetime import  datetime
import pandas as pd
from millify import millify
import streamlit_authenticator as stauth
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

try:
    fb = st.session_state['fb']
except:
    fb = process_data('processed_adsets.csv')
    st.session_state['fb'] = fb

try:
    hotmart = st.session_state['hotmart_data']
except:
    tmp_hotmart = get_data_from_bucket(bucket_name='dashboard_marketing_processed', file_name='processed_hotmart.parquet', file_type='.parquet')
    raw_hotmart = pd.read_parquet(BytesIO(tmp_hotmart), engine='pyarrow')
    raw_hotmart['count'] = 1
    raw_hotmart['tracking.source_sck'] = raw_hotmart['tracking.source_sck'].fillna(value='Desconhecido')
    raw_hotmart['order_date'] = pd.to_datetime(raw_hotmart['order_date'])
    raw_hotmart['approved_date'] = pd.to_datetime(raw_hotmart['approved_date'])
    st.session_state['hotmart_data'] = raw_hotmart
    hotmart = st.session_state['hotmart_data'] = raw_hotmart



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
    st.title('Desempenho em picos de venda')

    ##################### FILTERS ######################
    option = st.sidebar.selectbox(label='Selecione o pico desejado', options=['Vitalício/2024'])
    
    if option == 'Vitalício/2024':
        date_range = [datetime.strptime('2024-02-15', '%Y-%m-%d').date(), datetime.strptime('2024-02-29', '%Y-%m-%d').date()]
    
    else:
        date_range = [hotmart['approved_date'].min(), hotmart['approved_date'].max()]
    
    limited_hotmart = hotmart.loc[(hotmart['order_date'] >= date_range[0]) & (hotmart['order_date'] <= date_range[1])]
    
    limited_fb = fb.loc[(fb['date'] >= date_range[0]) & (fb['date'] <= date_range[1])]

    ##################### BEGIN ########################
    g_hotmart = limited_hotmart.loc[(limited_hotmart['status'].isin(['APPROVED', 'COMPLETE'])) & (limited_hotmart['source'] == 'PRODUCER'), 
                                    ['approved_date', 'commission.value']].groupby(by='approved_date').sum().copy()


    g_fb = limited_fb[['date', 'spend']].groupby(by='date').sum()
    g_data = g_hotmart.merge(g_fb, left_index=True, right_index=True, how='left')
    g_data.columns = ['Faturamento', 'Investimento FB Ads']

    g_data['Lucro'] = g_data['Faturamento'] - g_data['Investimento FB Ads']
    g_data['ROAS'] = g_data['Lucro'] / g_data['Investimento FB Ads']

    st.write(f'Periodo considerado {g_data.index.min().date()} - {g_data.index.max().date()}')
    col_1, col_2, col_3, col_4 = st.columns(4)
    with col_1:
        st.metric(label='Faturamento', value=f'R$ {millify(g_data["Faturamento"].sum(), precision=1)}')

    with col_2:
        st.metric(label='Investimento em Ads (Facebook)', value=f'R$ {millify(g_data["Investimento FB Ads"].sum(), precision=1)}')

    with col_3:
        st.metric(label='Lucro', value=f'R$ {millify(g_data["Lucro"].sum(), precision=1)}')

    with col_4:
        st.metric(label='ROAS', value=round(g_data['Lucro'].sum() / g_data['Investimento FB Ads'].sum(), 2))

    hist_plot = px.line(g_data, x=g_data.index, y=['Faturamento', 'Investimento FB Ads','Lucro'], title='Evolução diária do Faturamento/Investimento')
    st.plotly_chart(hist_plot, True)

    st.divider()
    late_col1, late_col2 = st.columns(2)

    with late_col1:
        st.metric(f'Melhor dia {g_data.index[g_data["Faturamento"].argmax()].date()}', value=f'R${millify(g_data["Faturamento"].max(), precision=1)}')

    with late_col2:
        st.metric(f'Pior dia {g_data.index[g_data["Faturamento"].argmin()].date()}', value=f'R${millify(g_data["Faturamento"].min(), precision=1)}')

    
    tmp = limited_hotmart.loc[(limited_hotmart['status'].isin(['COMPLETE','APPROVED'])) & (limited_hotmart['source'] == 'PRODUCER')].copy()
    tmp.loc[(tmp['tracking.source_sck'].str.contains('mail'))
             & ((tmp['tracking.source_sck'].str.contains('upgrade'))), 'tracking.source_sck'] = 'e-mail upgrade'
    tmp.loc[(tmp['tracking.source_sck'].str.contains('mail'))&(~(tmp['tracking.source_sck'].str.contains('upgrade'))), 'tracking.source_sck'] = 'e-mail'
    tmp.loc[(tmp['tracking.source_sck'].str.contains('venda'))
            & (tmp['tracking.source_sck'].str.contains('upgrade')), 'tracking.source_sck'] = 'vendas upgrade'
    tmp.loc[(tmp['tracking.source_sck'].str.contains('venda'))
            & (~tmp['tracking.source_sck'].str.contains('upgrade')), 'tracking.source_sck'] = 'vendas'
    tmp.loc[tmp['tracking.source_sck'].str.contains('home'), 'tracking.source_sck'] = 'home'
    tmp.loc[tmp['tracking.source_sck'].str.contains('popup'), 'tracking.source_sck'] = 'popup'
    

    # Get unique sck values
    sck_values = tmp['tracking.source_sck'].unique()
    fig = make_subplots(rows=1, cols=2, column_titles=['Distribuição das vendas', 'Distribuição do faturamento'],
                        shared_yaxes=True, specs=[[{"type": "pie"}, {"type": "pie"}]])
    # Define a color map based on sck values
    color_map = {sck: px.colors.qualitative.Light24[i % len(px.colors.qualitative.Light24)] for i, sck in enumerate(sck_values)}

    # Create the first pie chart
    trace1 = go.Pie(values=tmp['count'], labels=tmp['tracking.source_sck'],domain=dict(x=[0, 0.5]), hole=0.4)
    colors1 = [color_map.get(sck, '#1f77b4') for sck in tmp['tracking.source_sck']]
    trace2 = go.Pie(values=tmp['commission.value'], labels=tmp['tracking.source_sck'],domain=dict(x=[0.51, 1.0]), hole=0.4)

    # Set colors using color_map
    colors2 = [color_map.get(sck, '#1f77b4') for sck in tmp['tracking.source_sck']]
    trace2.marker.colors = colors1
    trace1.marker.colors = colors1
    
    fig.add_traces([trace1, trace2])
    fig.update_layout(height=600, showlegend=True)
  
    st.plotly_chart(fig, use_container_width=True)

