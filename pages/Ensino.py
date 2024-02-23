import streamlit as st
import re
import numpy as np
from datetime import datetime, timedelta
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from google.oauth2 import service_account
from concurrent.futures import ThreadPoolExecutor
import typing
import streamlit_authenticator as stauth
from pytz import timezone, utc
from time import perf_counter

tables = [
        'users',
        'usermeta',
        'activities',
        'courses_completed',
        'trails_completed',
        'credits',
        'posts',
        'postmeta',
        'terms',
        'term_relationships',
        'term_taxonomy']


# ===== Data Load =====
@st.cache_data(ttl=60000)
def run_query(tables) -> dict[typing.Any, pd.DataFrame]:
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    def read_csv(table) -> pd.DataFrame:
        df = pd.read_gbq(f"SELECT * FROM hub_data.{table}", 'scidata-299417', use_bqstorage_api=True, credentials=credentials)
        return df
    
    def read_dataframes_in_parallel(tables):
        with ThreadPoolExecutor() as executor:
            dataframes = list(executor.map(read_csv, tables))
        return dataframes

    list_dfs = read_dataframes_in_parallel(tables)
    return {i: j for i, j in zip(tables, list_dfs)}

@st.cache_data
def process_hotmart(raw_hotmart) -> pd.DataFrame:
    """
    Process raw_hotmart (from processed hotmart_data on Google Storage Bucket/ dashboard_marketing_processed)
    to fit the needs of Ensino dashboard.
    """
    df_hotmart = pd.DataFrame()
    df_hotmart["date"] = raw_hotmart['order_date'].copy()
    df_hotmart["price"] = raw_hotmart['commission.value'].copy()
    df_hotmart["status"] = raw_hotmart['status'].copy()
    df_hotmart["transaction"] = raw_hotmart['transaction'].copy()
    df_hotmart["email"] = raw_hotmart['email'].copy()
    df_hotmart["name"] = raw_hotmart['buyers_name'].copy()
    df_hotmart["tracking"] = raw_hotmart['tracking.source_sck'].copy()
    return df_hotmart

# ===== Data Process =====
@st.cache_data(ttl=6000)
def get_hub_users() -> pd.DataFrame:
    usermeta_ids = dfs["usermeta"]["user_id"].value_counts().index
    user_ids = list(set(dfs["users"].index).intersection(usermeta_ids))
    users = []

    for id_ in user_ids:
        users.append({"id": id_})
        for row in dfs["usermeta"][dfs["usermeta"]["user_id"] == id_].values:
            users[-1][row[2]] = row[3]
        users[-1]['email'] = dfs["users"].loc[id_]['user_email']
        users[-1]['user_registered'] = dfs["users"].loc[id_]['user_registered']
    df_users_hub = pd.DataFrame(users)

    padrao = r'"([^"]*)"'
    df_users_hub.loc[:, "role"] = df_users_hub["jaikj_capabilities"].apply(lambda x: re.search(padrao, x).group(1))
    return df_users_hub

def process_activities():
    dict_posts_temp = dfs["posts"].set_index("ID").to_dict()
    dfs["activities"]["post_title"] = dfs["activities"]["post_id"].map(dict_posts_temp["post_title"])
    dfs["activities"]["post_name"] = dfs["activities"]["post_id"].map(dict_posts_temp["post_name"])
    dfs["activities"]["post_type"] = dfs["activities"]["post_id"].map(dict_posts_temp["post_type"])
    dfs["activities"]["created_at"] = pd.to_datetime(dfs["activities"]["created_at"]).dt.tz_localize(None).dt.tz_localize(('America/Sao_Paulo'))
    dfs["activities"]["updated_at"] = pd.to_datetime(dfs["activities"]["updated_at"]).dt.tz_localize(None).dt.tz_localize('America/Sao_Paulo')

    # ID casa com object_id do dfs["term_relationships"]
    df_posts = dfs["posts"][["ID", "post_title", "post_type"]]
    dict_temp = dfs["terms"].set_index("term_id").to_dict()
    dfs["term_taxonomy"]["name"] = dfs["term_taxonomy"]["term_taxonomy_id"].map(dict_temp["name"])
    dfs["term_taxonomy"]["slug"] = dfs["term_taxonomy"]["term_taxonomy_id"].map(dict_temp["slug"])

    dfs["term_relationships"]["post_name"] = dfs["term_relationships"]["object_id"].map(df_posts.set_index("ID").to_dict()["post_title"])
    dfs["term_relationships"]["post_type"] = dfs["term_relationships"]["object_id"].map(df_posts.set_index("ID").to_dict()["post_type"])
    dfs["term_relationships"]["taxonomy_name"] = dfs["term_relationships"]["term_taxonomy_id"].map(dfs["term_taxonomy"].set_index("term_taxonomy_id").to_dict()["name"])
    dfs["term_relationships"]["taxonomy"] = dfs["term_relationships"]["term_taxonomy_id"].map(dfs["term_taxonomy"].set_index("term_taxonomy_id").to_dict()["taxonomy"])
    dfs["term_relationships"]["count"] = dfs["term_relationships"]["term_taxonomy_id"].map(dfs["term_taxonomy"].set_index("term_taxonomy_id").to_dict()["count"])

    dfs["activities"].loc[:, "curso"] = dfs["activities"]["post_id"].map(dfs["term_relationships"][dfs["term_relationships"]["taxonomy"] == "curso"].set_index("object_id").to_dict()["taxonomy_name"])
    # dfs["activities"]["curso"].fillna("", inplace=True)
    dfs["activities"]["curso"] = dfs["activities"]["curso"].apply(lambda x: x.replace('&amp;', '&') if type(x) == type(str) else x)
    
    projects_query = (dfs["activities"]["post_type"] == "atividade") & (dfs["activities"]["curso"].isna())
    dfs["activities"].loc[projects_query, "projeto"] = dfs["activities"].loc[projects_query, "post_id"].map(dfs["term_relationships"][dfs["term_relationships"]["taxonomy"] == "project"].set_index("object_id").to_dict()["taxonomy_name"])
    st.session_state["course_ids"] = dfs["posts"][dfs["posts"]["post_type"] == "curso"].set_index("post_title").to_dict()["ID"]

def process_hub_users(df_users_hub, df_hotmart) -> tuple:
    # Users treatment
    df_users_hub = df_users_hub[["id", "first_name", "last_name", "email", "role", "user_registered"]]
    df_users_hub.loc[:, "credits"] = df_users_hub.index.map(dfs["credits"].set_index("user_id").to_dict()["credits"])
    df_users_hub["buyer"] = df_users_hub["email"].isin(df_hotmart["email"])
    df_users_hub["buyer_date"] = df_hotmart.loc[df_hotmart['email'].isin(df_users_hub['email']), 'date'].copy()
    df_users_hub['buyers_date_datetime'] = pd.to_datetime(df_users_hub["buyer_date"])
    saopaulo = timezone('America/Sao_Paulo')
    df_users_hub['buyers_date_datetime'] =  df_users_hub['buyers_date_datetime'].apply(lambda x: saopaulo.localize(x) if pd.notnull(x) else x)
    df_users_hub["tracking"] = df_hotmart.loc[df_hotmart['email'].isin(df_users_hub['email']), 'tracking'].copy()
    df_users_hub["total_activities"] = dfs["activities"].groupby("user_id")[["id"]].count().to_dict()["id"]

    
    df_users_hub.loc[:, "last_activity"] = pd.to_datetime(df_users_hub["id"].map(dfs["activities"].groupby("user_id")[["updated_at"]].last().to_dict()["updated_at"]))
    df_users_hub["days_to_buy"] = (df_users_hub['buyers_date_datetime'] - df_users_hub["user_registered"]).apply(lambda x: x.days).fillna(0) #Não deveria ser outro valor? Pode ser que algum se inscreva e compre no mesmo dia
    df_users_hub["days_registered"] = datetime.now().date() - df_users_hub["user_registered"].apply(lambda x: x.date())

    df_users_hub.loc[:, "total_days"] = (datetime.now(tz=saopaulo) - df_users_hub["user_registered"]).apply(lambda x: x.days)
    df_users_hub["idle_days"] = datetime.now(tz=saopaulo) - df_users_hub["last_activity"].dt.tz_convert('America/Sao_Paulo') #garantindo o fuso-horário
    df_users_hub["idle_days"] = df_users_hub["idle_days"].apply(lambda x: x.days)
    df_users_hub["used_days"] = (df_users_hub["total_days"] - df_users_hub["idle_days"]).fillna(0)

    df_users_hub["weeks_to_buy"] = df_users_hub["days_to_buy"].apply(lambda x: int(x / 7))#.apply(lambda x: x if x < 20 else 20)  Porque limitar a 20 semanas?
    df_users_hub["total_weeks"] = df_users_hub["total_days"].apply(lambda x: int(x / 7))#.apply(lambda x: x if x < 20 else 20)
    df_users_hub["buyer"] = df_users_hub["email"].isin(df_hotmart["email"]).apply(lambda x: 1 if x else 0)

    df_users_hub = df_users_hub.sort_values(by="buyer_date")
    df_subs = df_users_hub[df_users_hub["role"] == "subscriber"]
    df_pro = df_users_hub[df_users_hub["role"] == "pro"]
    return df_users_hub, df_subs, df_pro

def get_course_data(df_act) -> dict:
    course_ids = dfs["posts"].query("post_type=='curso'")["ID"].values
    # pdb.set_trace()
    dict_course = {}
    df_act_count = df_act.groupby("post_id").count()["grade"]
    dict_posts_name = dfs["posts"].set_index("ID").to_dict()["post_title"]

    for cid in course_ids:
        dfs["postmeta"].query(f"post_id=={cid}")
        dict_course[cid] = {}
        for row in dfs["postmeta"].query(f"post_id=={cid}").values:
            if "modules" in row[2] and "title" in row[2] and row[2][0] != "_":
                dict_course[cid][int(row[2].split("_")[1])] = {}
                dict_course[cid][int(row[2].split("_")[1])]["name"] = row[3]
            
            if "modules" in row[2] and "activities" in row[2] and row[2][0] != "_":
                result = re.findall(r'"[^"]*"',row[3])
                
                results = [int(s[1:-1]) for s in result]
                dict_course[cid][int(row[2].split("_")[1])]["activities"] = {}
                
                for i, result in enumerate(results):
                    if result in df_act_count.index:
                        dict_act_temp = {
                            "name": dict_posts_name[result], 
                            "count": df_act_count[result], 
                            "id": result}
                        dict_course[cid][int(row[2].split("_")[1])]["activities"][i] = dict_act_temp

    # Obtem a contagem de atividades por modulo
    for cid in dict_course.keys():
        for act in dict_course[cid].keys():
            activities_sum = sum(item['count'] for item in dict_course[cid][act]["activities"].values())
            dict_course[cid][act]["count"] = activities_sum
    return dict_course

def get_data_churnrate_plot(df, max_idle_days=30, max_total_days=180) -> pd.DataFrame:
    """
    Prepares the data for churnrate hist plotting, by adding a aditional column 'abandoned' if that user has idle_days > max_idle_days
    and filtering the df so only users with total_days < max_total_days are considered.
    """
    tmp = df.copy()
    tmp['abandoned'] = tmp.apply(lambda x: 1 if x['idle_days'] > max_idle_days else 0, axis=1)
    tmp = tmp.loc[tmp['total_days'] < max_total_days]
    return tmp

@st.cache_data    
def get_courses_by_tracks(dfs: dict)-> dict:
    """
    Dado o dicionário dfs contendo as tabelas do wp como Dataframe, retorna um dicionário contendo
    a qual trilha cada curso pertence.
    """
    tracks_ids = dfs['term_taxonomy'].loc[dfs["term_taxonomy"]['taxonomy'] == 'track', 'term_taxonomy_id']
    tracks_courses = {}
    a = {}

    for track_id in tracks_ids.unique():
        tracks_courses[str(track_id)] = dfs["term_relationships"].loc[dfs['term_relationships']['term_taxonomy_id'] == track_id, 'object_id'].to_frame().merge(dfs['posts'][['ID', 'post_title']], left_on='object_id', right_on='ID', how='inner')['post_title'].values
    
    all_courses = tracks_courses.values()
    all_courses_flatten = [item for sublist in all_courses for item in sublist]
    tmp_counter = pd.Series(all_courses_flatten).value_counts().to_frame()
    common_values = tmp_counter.drop(tmp_counter.loc[tmp_counter['count'] < 2].index).index.to_numpy()
    for key in tracks_courses.keys():
        tracks_courses[key] = tracks_courses[key][~np.isin(tracks_courses[key], common_values)]
    
    tracks_courses['Multiplas'] = common_values
    return tracks_courses

def get_color_codes(df, track_courses_dict):
    trilhas = np.full(shape=df.shape[0], fill_value='Sem trilha')
    
    for key in track_courses_dict.keys():
        trilhas[np.isin(df['curso'], track_courses_dict[key])] = str(key)
    return trilhas


def get_completion_number(df_users, df_act, period, course):
    # Filter users who started the course
    users_started_course = df_users[df_users['id'].isin(df_act.loc[df_act['curso'] == course, 'user_id'])]

    # Get unique course activities
    course_act = set(df_act.loc[df_act['curso'] == course, 'post_id'])

    # Filter activities for the specific course
    df_act_course = df_act[df_act['curso'] == course]

    # Calculate time difference for each user
    time_difference = (df_act_course.groupby('user_id')['updated_at'].max() - df_act_course.groupby('user_id')['created_at'].min()).dt.days

    # Filter users who completed the course within the specified period
    completed_course_users = users_started_course[
        users_started_course['id'].isin(time_difference[time_difference <= period].index) &
        users_started_course['id'].apply(lambda u_id: course_act.issubset(df_act_course.loc[df_act_course['user_id'] == u_id, 'post_id']))
    ]['id'].tolist()

    return completed_course_users

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
    st.title('Dados Ensino')

    try:
        hotmart = st.session_state['hotmart_data']
    except:
        st.write('Carregar dados na página da Hotmart')

    # Data Load
    dfs = run_query(tables)
    df_hotmart = process_hotmart(raw_hotmart=hotmart)
    dfs["users"]["user_registered"] = pd.to_datetime(dfs["users"]["user_registered"]).dt.tz_convert('America/Sao_Paulo')
    dfs["users"].set_index("ID", inplace=True)
    del dfs["term_relationships"]["term_order"], dfs["term_taxonomy"]["term_id"], dfs["term_taxonomy"]["description"]

    df_users_hub = get_hub_users()
    process_activities()
    df_users_hub, df_subs, df_pro = process_hub_users(df_users_hub=df_users_hub, df_hotmart=df_hotmart)
    #st.write(df_users_hub.head())

    #pdb.set_trace()

    # ============================
    # Controladores
    # ============================
    all_courses = dfs["activities"]["curso"].unique()
    date_range = st.sidebar.date_input("Datas", value=(datetime.today()-timedelta(days=15), datetime.today()))
    role = st.sidebar.selectbox("Role", ["subscriber", "pro"])


    cids = sorted(list(st.session_state["course_ids"].keys()))
    course = st.sidebar.selectbox("Curso", cids, cids.index("Python Starter"))
    course_id = st.session_state["course_ids"][course] # Dict nome_curso : id
    courses_by_track_dict = get_courses_by_tracks(dfs) 


    # Filtro de data e Roles
    df_act_ = dfs["activities"][(dfs["activities"]["created_at"].dt.date >= date_range[0]) & (dfs["activities"]["created_at"].dt.date <= date_range[-1])]
    df_users_filt = df_users_hub[df_users_hub["role"] == role]
    df_users_filt_register = df_users_filt[(df_users_filt["user_registered"].dt.date >= date_range[0]) & (df_users_filt["user_registered"].dt.date <= date_range[-1])]
    user_ids = df_users_filt["id"].unique()
    df_act_ = df_act_[df_act_["user_id"].isin(values=user_ids)]
    #st.write(f'df_act {df_act_.head()}')
    df_act_['Trilha'] = get_color_codes(df=df_act_, track_courses_dict=courses_by_track_dict) #Adicionando a coluna das trilhas
    dict_course = get_course_data(df_act=df_act_)


    # explorar dict_course

    # ============================
    # Data Display
    # ============================
    total_users_all_time = len(df_users_filt)
    total_users_date_range = len(df_users_filt_register)

    col1, col2, col3 = st.columns(3)
    col1.metric(label=f"Total: {role.capitalize()}", value=f"{total_users_all_time}")
    col2.metric(label=f"Total registrado no periodo: {role.capitalize()}", value=f"{total_users_date_range}")

    churn_rate_expt = st.expander('Churnrate Hists')
    with churn_rate_expt:
        max_idle = st.number_input(label='Limite para dias inativos', min_value=1, value=30)
        max_days = st.number_input(label='Limite para dias de registro', min_value=1, value=180)
        churn_data = get_data_churnrate_plot(df=df_users_hub, max_idle_days=max_idle, max_total_days=max_days)
        col_1, col_2, col_3 = st.columns(3)
        with col_1:
            pro_ = churn_data.loc[churn_data['role'] == 'pro']
            pro_hist = px.histogram(data_frame=pro_, x='total_days', y='abandoned', histnorm='percent', cumulative=True, title='Taxa cumulativa de churnrate para usuarios pro')
            st.plotly_chart(pro_hist, use_container_width=True)
        
        with col_2:
            subs_ = churn_data.loc[churn_data['role'] == 'subscriber']
            subs_hist = px.histogram(data_frame=subs_, x='total_days', y='abandoned', histnorm='percent', cumulative=True, title='Taxa cumulativa de churnrate para usuarios subscriber')
            st.plotly_chart(subs_hist, use_container_width=True)
        
        with col_3:
            basic_ = churn_data.loc[churn_data['role'] == 'basic']
            basic_hist = px.histogram(data_frame=basic_, x='total_days', y='abandoned', histnorm='percent', cumulative=True, title='Taxa cumulativa de churnrate para usuarios basic')
            st.plotly_chart(basic_hist, use_container_width=True)
    
    # Cursos mais assistidos
    with st.expander("Cursos e Trilhas assistidas"):
        courses = st.multiselect("Cursos a excluir", all_courses, ["Python Starter"])
        df_act_cut = df_act_[~df_act_["curso"].isin(values=courses)]
        col1, col2 = st.columns(2)
        activities_done_df = df_act_cut["curso"].value_counts().to_frame().merge(df_act_cut[['curso', 'Trilha']], on='curso', how='inner')
        activities_done_df.drop_duplicates(inplace=True)
        labels={'144':'Python Office', '145':'Análise e Visualização de Dados', '146':'Visão Computacional com Python',
                '147':'Data Science &amp; Machine Learning','148':'Trading Quantitativo', '149':'Dashboards Interativos com Python',
                'Multiplas': 'Múltiplas', 'sem trilha': 'Sem trilha'}
        activities_done_df['Trilha'] = activities_done_df['Trilha'].map(labels)
        color_map = {label: px.colors.qualitative.Plotly[i] for i, label in enumerate(labels.values())}

        fig = px.bar(activities_done_df, x=activities_done_df['count'], y='curso',color='Trilha',color_discrete_map=color_map)
        fig.update_layout(height=900, showlegend=False).update_yaxes(categoryorder="total ascending")
        col1.plotly_chart(fig, use_container_width=True)

        # Trilhas mais assistidas
        for course_, total in df_act_cut["curso"].value_counts().items():
            course_ = course_
            dfs["term_relationships"].loc[dfs["term_relationships"]["post_name"] == course_, "total_done"] = total
        
        df_terms_ = dfs["term_relationships"][~dfs["term_relationships"]["post_name"].isin([i for i in courses])]
        df_tracks = (df_terms_[df_terms_["taxonomy"] == 'track'].groupby("taxonomy_name")["total_done"].sum())
        df_tracks = df_tracks.to_frame().reset_index()
        df_tracks.columns = ['Trilhas', 'total_done']
        fig2 = px.bar(df_tracks, x='total_done', y='Trilhas', color='Trilhas', color_discrete_map=color_map).update_yaxes(categoryorder="total ascending")
        fig2.update_layout(height=900, showlegend=False)
        col2.plotly_chart(fig2, use_container_width=True)


        # Análise de atividades
        list_modules = []
        for module in range(max(dict_course[course_id].keys())+1):
            dados = [(j["name"], j["count"], module) for i, j in dict_course[course_id][module]["activities"].items()]
            list_modules += dados
        df = pd.DataFrame(list_modules, columns=['name', 'count', 'module'])

    # pdb.set_trace()
    curso_exp = st.expander('Detalhes - curso')
    with curso_exp:
        total_users_course = len(set(df_act_[df_act_["curso"] == course]["user_id"].unique()).intersection(set(user_ids)))
        total_users_act = len(df_act_['user_id'].unique())

        st.metric(label=f"Usuários fazendo o curso no período", value=f"{total_users_course}")
        st.metric(label=f'Taxa de usuários {role} que iniciaram atividades no curso {course} em relação a todos os cursos', value=round((total_users_course/total_users_act), 3))
        delta = st.number_input(label="Selecione o período para completar o curso", value=15, min_value=1)
        completed_number = len(get_completion_number(df_users=df_users_filt, df_act=dfs["activities"], period=delta, course=course))
        completed_rate = completed_number/len(dfs["activities"].loc[dfs['activities']['curso'] == course, 'user_id'].unique())
        st.metric(label=f'Total de usuários {role} que completaram o curso no periode de {delta} dias', value=completed_number)
        st.metric(label=f'Taxa histórica de usuários {role} que completaram o curso no periodo de {delta} dias', value=round(completed_rate,2))
        # df["count"] =  df["count"] /  total_users

        tmp_df = df_act_.loc[(df_act_["curso"] == course) & (df_act_['user_id'].isin(user_ids))]
        tmp_df = tmp_df[['user_id', 'post_id']].groupby(by='user_id').count()
        tmp_df.columns = ['Total_activities']
        tmp_df = tmp_df.merge(df_users_filt_register[['id', 'user_registered']], left_index=True, right_on='id', how='inner') # inner para limitar os usuarios do periodo selecionado
        tmp_df['days_since_reg'] = (datetime.now().astimezone() - tmp_df['user_registered']).dt.days
        hist_fig = px.histogram(data_frame=tmp_df[['days_since_reg', 'Total_activities']], 
                                y='days_since_reg', x='Total_activities', histfunc='avg', text_auto='d', marginal='box',
                                title='Histograma dos módulos completos por tempo de registro', 
                                labels={'Total_activities':'Nº de atividades completas do módulo'}).update_layout(yaxis_title='Nº médio de dias registrado', bargap=0.15)
        hist_fig.update_yaxes(showgrid=False)
        st.plotly_chart(hist_fig, use_container_width=True)

        df["name"] = df.index.astype(str) + " - " + df["name"]
        df.set_index(['name'], inplace=True)
        fig4 = px.bar(df, color='module', title='Número de usúarios que iniciaram a atividade no período').update_layout(yaxis_title='Número de usuários')
        fig4.update_layout(height=800, showlegend=False)
        st.plotly_chart(fig4, use_container_width=True)

        # Análise de módulos
        dados = [(f"{key} - " + val['name'], val['count'], len(val["activities"])) for key, val in dict_course[course_id].items()]
        # len(dict_course[course_id][0]["activities"])
        df = pd.DataFrame(dados, columns=['name', 'count', 'module_size'])
        df["potential_size"] = df["module_size"] * total_users_course
        # pdb.set_trace()
        df["count"] = df["count"] / df["potential_size"]
        df.set_index(['name'], inplace=True)
        fig3 = px.bar(df, y="count")
        fig3.update_layout(height=600, showlegend=False, title='Distribuição do potencial de cada módulo (?)')
        st.plotly_chart(fig3, use_container_width=True)
