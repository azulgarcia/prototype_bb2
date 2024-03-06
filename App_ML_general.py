import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

df = pd.read_csv("data/bb2_general_performances.csv")

df['date'] = pd.to_datetime(df['date'])

df['date'] = pd.to_datetime(df['date']).dt.date

st.markdown('### Performances projects ML General')

#category_filter = st.selectbox('Select category:', df['category'].unique())

rango_fechas = pd.date_range(df['date'].min(), df['date'].max())
fecha_inicio_default = max(rango_fechas.min().date(), df['date'].min())
fecha_fin_default = min(rango_fechas.max().date(), df['date'].max())
fecha_inicio = st.date_input("Select start date",
                             min_value=rango_fechas.min().date(),
                             max_value=rango_fechas.max().date(),
                             value=fecha_inicio_default)
fecha_fin = st.date_input("Select end date",
                          min_value=rango_fechas.min().date(),
                          max_value=rango_fechas.max().date(),
                          value=fecha_fin_default)

num_projects = st.slider('Select the number of projects to consider by date:',
                                min_value=1, max_value=100, value=10)

df_filtrado = df[(df['date'] >= fecha_inicio) & (df['date'] <= fecha_fin)]

df_filtrado_sort = df_filtrado.sort_values(by=['date', 'rank'], ascending=[True, True])

df_filtrado_final = df_filtrado_sort.groupby('date').head(num_projects)

grouped_df = df_filtrado_final.groupby('date')['performance'].mean().reset_index()

init_investment = 1000
roi_total = []

accumulated_roi = init_investment
for index, row in grouped_df.iterrows():
    accumulated_roi = accumulated_roi * (1 + row['performance'])
    roi_total.append(accumulated_roi)

grouped_df['return_total'] = roi_total

fig_roi = px.line(grouped_df, x='date', y='return_total', markers=True, labels={'performance': 'Mean Performance', 'date': 'Date'})
fig_roi.update_traces(line_color='orange')
fig_roi.add_trace(
    go.Scatter(
        x=[df_filtrado_final['date'].min(), df_filtrado_final['date'].max()],
        y=[1000, 1000],
        mode='lines',
        name='Initial Investment',
        line=dict(color="black", width=2, dash="dash"),
    )
)

st.markdown("##### Return on investment of 1000 dollars")
st.plotly_chart(fig_roi)

grouped_df_performance = df_filtrado_final.groupby('date')['performance'].mean().reset_index()
fig_performance = px.line(grouped_df_performance, x='date', y='performance', markers=True,
                          labels={'performance': 'Mean Performance', 'date': 'Date'})
fig_performance.update_traces(line_color='green')

st.markdown("##### Mean performance")
st.plotly_chart(fig_performance)

default_year = 2024
default_week = 7

### table
year_filter = st.selectbox('Select year:', df['year'].unique(), index=df['year'].unique().tolist().index(default_year))
week_filter = st.number_input('Select week:', min_value=int(df['week'].min()), max_value=int(df['week'].max()),value=default_week)

filter_df = df[((df['year'] == year_filter) & (df['week'] == week_filter))]

filter_df_sort = filter_df.sort_values(by='rank', ascending=True)

filter_df_final = filter_df_sort.head(num_projects).reset_index()

columns_to_display = ['date', 'project_name', 'start_price', 'end_price', 'performance', 'rank']

st.write(filter_df_final[columns_to_display])

