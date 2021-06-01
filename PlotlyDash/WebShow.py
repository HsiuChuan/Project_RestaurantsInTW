import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input
import plotly.express as px
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import numpy as np
import pymysql.cursors
import pandas as pd
import googlemaps
pd.options.mode.chained_assignment = None  # default='warn'

## Load data from heroku mysql
mydb = pymysql.connect(host='us-cdbr-east-03.cleardb.com',
                       user='',
                       password='',
                       db='heroku_6e64764cb9b4e3a',
                       charset='utf8mb4',
                       cursorclass=pymysql.cursors.DictCursor)
cursor = mydb.cursor()
stores_sql = "SELECT * FROM store_info "
stores = cursor.execute(stores_sql) # , values)
stores = cursor.fetchall()

# get the header of sql
field_names = [i[0] for i in cursor.description]
mydb.commit()
cursor.close()

# Convert tuple to dataframe
df = pd.DataFrame(stores, columns=field_names)
df = df[~df['related_place'].isnull()]
country = ['臺北市', '新北市', '基隆市', '桃園市', '新竹縣', '新竹市', '苗栗縣', '臺中市', '南投縣',
           '彰化縣', '雲林縣', '嘉義縣', '嘉義市', '臺南市', '高雄市', '屏東縣', '宜蘭縣', '花蓮縣',
           '臺東縣', '澎湖縣', '金門縣', '連江縣']

uni_keys = df['key1'].unique().tolist() + df['key2'].unique().tolist() + df['key3'].unique().tolist()

# ===========================================================================================
# map2-figure
mapbox_access_token = "pk.eyJ1IjoiaGhzdSIsImEiOiJja290dG9vYTkwY3JxMndrN2RiZ2Q3aWJyIn0.zgfIK0Wtlu08r60x4sfsyw"
fig = px.scatter_mapbox(df, lat="lat", lon="lng", hover_name="name",
                        color_continuous_scale=px.colors.cyclical.IceFire,
                        zoom=5.5, height=330)
fig.update_layout(mapbox=dict(accesstoken=mapbox_access_token))  # mapbox_style
fig.update_layout(margin={"r": 0, "t": 0, "l": 2, "b": 0})

# hist2-figure
df['country'] = df['related_place'].str[:3]
dfG = df.groupby(['country']).count().sort_values("name", ascending=False)
dfG = dfG.rename(columns={'country':'Country'})

dfG = dfG.rename(columns = {'name':'Number of Stores'})
fig2 = px.bar(dfG, y='Number of Stores', x=dfG.index, text='Number of Stores')
fig2.update_traces(textposition='outside',marker_color='rgb(158,202,225)', marker_line_color='rgb(8,48,107)',
                  marker_line_width=1.5, opacity=0.6)
fig2.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')

# hist3-figure
bins = [0, 3.0, 3.5, 4, 4.5, 5]
labels = ['below 3', '3.0 ~ 3.5','3.5 ~ 4.0', '4.0 ~ 4.5', '4.5 ~ 5.0']
dfBinned = df[df['user_ratings_total'] > 25]
dfBinned['Rating Level'] = pd.cut(dfBinned['rating'], bins=bins, labels=labels)
dfBinned = dfBinned.groupby(['Rating Level']).count().sort_index()
dfBinned = dfBinned.rename(columns = {'name':'Number of Stores'})

fig3 = px.bar(dfBinned, x='Number of Stores', y=dfBinned.index, text='Number of Stores', orientation='h')
fig3.update_traces(textposition='outside',marker_color='rgb(66,209,179)', marker_line_color='rgb(0,155,76)',
                  marker_line_width=1.5, opacity=0.6)
fig3.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')

# hist4-figure
bins = [0, 100, 300, 500, 1000, 3000, 5000, 10000, np.inf]
labels = ['below 100', '100 ~ 300','300 ~ 500', '500 ~ 1000', '1000 ~ 3000', '3000 ~ 5000', '5000 ~ 10000', 'above 10000']
dfUN = df[df['user_ratings_total'] > 25]
dfUN['Total Users'] = pd.cut(dfUN['user_ratings_total'], bins=bins, labels=labels)
dfUN = dfUN.groupby(['Total Users']).count().sort_index()
dfUN = dfUN.rename(columns={'name':'Number of Stores'})

fig4 = px.bar(dfUN, x='Number of Stores', y=dfUN.index, text='Number of Stores', orientation='h')
fig4.update_traces(textposition='outside',marker_color='rgb(66,209,179)', marker_line_color='rgb(0,155,76)',
                  marker_line_width=1.5, opacity=0.6)
fig4.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')

# hist5-figure
dfK = df.rename(columns={'name':'Number of Stores', 'price_level':'Price Level'})
dfK = dfK.groupby(['Price Level']).count().sort_values("Number of Stores", ascending=False)

fig5 = px.bar(dfK, x='Number of Stores', y=dfK.index, text='Number of Stores', orientation='h')
fig5.update_traces(textposition='outside',marker_color='rgb(66,209,179)', marker_line_color='rgb(0,155,76)',
                  marker_line_width=1.5, opacity=0.6)
fig5.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')




# ===========================================================================================

## Create an app with themes
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP],
                meta_tags=[{'name': 'viewport',
                            'content': 'width=device-width, initial-scale=1.0'}]
                )
server = app.server

app.layout = dbc.Container([

    dbc.Row(
        dbc.Col(html.H1("Restaurants in Taiwan Dashboard",
                        className='text-center bg-white mb-4'),
                width=12)
    ),

# Filters
    dbc.Row(
        dbc.Col(html.H3("Finding Rrestaurants With Filters",
                        className='text-left mb-4'),
                width=12)
    ),
    dbc.Row([
        dbc.Col([
            dcc.Dropdown(id='Country-dpdn', placeholder="Select a city",
                         options=[{'label': x, 'value': x}
                                  for x in country]
                         ),
            dcc.Dropdown(id='PriceLevel-dpdn', placeholder="Select Price-Level",
                         options=[{'label': "Below Level " + str(x), 'value': x}
                                  for x in range(0,5)]
                         ),
            dcc.Dropdown(id='Rating-dpdn', placeholder="Select Rating",
                         options=[{'label': "Above " + str('{0:.1f}'.format(x)) + " Star", 'value': float('{0:.1f}'.format(x))}
                                  for x in list(np.arange(3, 5.2, 0.2))]
                         ),
            dcc.Dropdown(id='UsersNum-dpdn', placeholder="Select Total Rating Users",
                         options=[{'label': "Above " + str(x) + " People", 'value': x}
                                  for x in list(np.arange(0, 50000, 500))]
                         ),
            dcc.Dropdown(id='KeyWords-dpdn', placeholder="Select KeyWord",
                         options=[{'label': x[1:-1], 'value': x[1:-1]}
                                  for x in uni_keys]
                         )
            ], width=6)
        ], justify="center", align="center", className="h-50"),

    dbc.Row([
        dbc.Col([
            dcc.Graph(id='line-fig', figure={})
                    ],xs=12, sm=12, md=12, lg=6, xl=6
                #width={'size':6, 'offset':0, 'order':1}
                ),
        dbc.Col([
            dcc.Graph(id='map-fig', figure={})
        ], xs=12, sm=12, md=12, lg=6, xl=6
            # width={'size':5, 'offset':0, 'order':2}
        )
    ], no_gutters=False, align="center"),

# Google Place
    dbc.Row(
        dbc.Col(html.H3("Finding Rrestaurants Around You",
                        className='text-left mb-4'),
                width=12)
    ),
    dbc.Row([
        dbc.Col([
            dcc.Input(id='Place_ip',
                      placeholder='Input A Place',
                      debounce=True,
                      size='40')
        ])
    ], justify="center", align="center", className="h-50"),
    dbc.Row([
        dbc.Col([
            dcc.Graph(id='line2-fig', figure={})
        ], xs=12, sm=12, md=12, lg=6, xl=6
            # width={'size':6, 'offset':0, 'order':1}
        ),
        dbc.Col([
            dcc.Graph(id='map2-fig', figure={})
        ], xs=12, sm=12, md=12, lg=6, xl=6
            # width={'size':5, 'offset':0, 'order':2}
        )
    ], no_gutters=False, align="center"),

# Reference
    dbc.Row(
        dbc.Col(html.H3("References",
                        className='text-left mb-4'),
                width=12)
    ),
    dbc.Row([
        dbc.Col(html.H5("Total "+ str(len(df))+" Stores"),
                width=6),
        dbc.Col(html.H5("The store numbers distribution in Taiwan"),
                width=6)
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Graph(figure=fig)
        ], xs=12, sm=12, md=12, lg=6, xl=6
            # width={'size':6, 'offset':0, 'order':1}
        ),
        dbc.Col([
            dcc.Graph(figure=fig2)
        ], xs=12, sm=12, md=12, lg=6, xl=6
            # width={'size':5, 'offset':0, 'order':2}
        )
    ], no_gutters=False, align="center"),
    dbc.Row([
        dbc.Col(html.H5("Price Level Category"),
                width=4),
        dbc.Col(html.H5("Rating Level Category"),
                width=4),
        dbc.Col(html.H5("Total Users Category"),
                width=4)

    ]),
    dbc.Row([
        dbc.Col([
            dcc.Graph(figure=fig5)
        ], xs=12, sm=12, md=12, lg=4, xl=4
            # width={'size':6, 'offset':0, 'order':1}
        ),
        dbc.Col([
            dcc.Graph(figure=fig3)
        ], xs=12, sm=12, md=12, lg=4, xl=4
            # width={'size':5, 'offset':0, 'order':2}
        ),
        dbc.Col([
            dcc.Graph(figure=fig4)
        ], xs=12, sm=12, md=12, lg=4, xl=4
            # width={'size':5, 'offset':0, 'order':2}
        )
    ], no_gutters=False, align="center")

], fluid=True)


# ===========================================================================================
## Create a bar chart
@app.callback(
    dash.dependencies.Output('line-fig', 'figure'),
    [
        dash.dependencies.Input('Country-dpdn', 'value'),
        dash.dependencies.Input('PriceLevel-dpdn', 'value'),
        dash.dependencies.Input('Rating-dpdn', 'value'),
        dash.dependencies.Input('UsersNum-dpdn', 'value'),
        dash.dependencies.Input('KeyWords-dpdn', 'value')
    ]
)
def update_bargraph(country_slctd, PL_slctd, Ra_slctd, UM_slctd, Key_slctd):
    dff = df[df['related_place'].str[:3] == country_slctd]
    dff = dff[dff['price_level'] <= PL_slctd]
    dff = dff[dff['rating'] >= Ra_slctd]
    dff = dff[dff['user_ratings_total'] >= UM_slctd]
    dff = dff[(dff['key1'].str[1:-1] == Key_slctd) | (dff['key2'].str[1:-1] == Key_slctd) | (dff['key3'].str[1:-1] == Key_slctd)]

    dff_sorted = dff.sort_values(by=['user_ratings_total', 'rating'], ascending=False).head(7)

    bar_color2 = ['#e6f3f7', '#deeff5', '#d6ebf2', '#cde7f0', '#c5e3ed', '#bddfeb', '#b5dbe8', '#add8e6']
    bar_color2.reverse()

    fig = go.Figure(data=[go.Bar(x=dff_sorted['user_ratings_total'].astype(int).to_list(), y=list(range(1, 8)),
                                 text=dff_sorted['name'],
                                 orientation='h')])  # hovertext=dff_sorted['name'],
    fig.update_traces(marker_color=bar_color2, textposition='inside')
    fig.update_layout(yaxis=dict(title="Ranking", autorange="reversed"),
                      width=620, height=480, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                      xaxis=dict(title="Total Rating Users"))
    return fig

mapbox_access_token = " "
@app.callback(
    dash.dependencies.Output('map-fig', 'figure'),
    [
        dash.dependencies.Input('Country-dpdn', 'value'),
        dash.dependencies.Input('PriceLevel-dpdn', 'value'),
        dash.dependencies.Input('Rating-dpdn', 'value'),
        dash.dependencies.Input('UsersNum-dpdn', 'value'),
        dash.dependencies.Input('KeyWords-dpdn', 'value')
    ]
)
def update_TWmap(country_slctd, PL_slctd, Ra_slctd, UM_slctd, Key_slctd):
    dff = df[df['related_place'].str[:3] == country_slctd]
    dff = dff[dff['price_level'] <= PL_slctd]
    dff = dff[dff['rating'] >= Ra_slctd]
    dff = dff[dff['user_ratings_total'] >= UM_slctd]
    dff = dff[(dff['key1'].str[1:-1] == Key_slctd) | (dff['key2'].str[1:-1] == Key_slctd) | (dff['key3'].str[1:-1] == Key_slctd)]

    dff_sorted = dff.sort_values(by=['user_ratings_total', 'rating'], ascending=False).head(7)
    dff_sorted['Rating'] = dff_sorted["rating"].astype(float).tolist()
    fig = px.scatter_mapbox(dff_sorted, lat="lat", lon="lng", hover_name="name",
                            color='Rating',
                            size='Rating',
                            color_continuous_scale=px.colors.cyclical.IceFire,
                            zoom=9, height=330)
    fig.update_layout(mapbox=dict( accesstoken=mapbox_access_token)) # mapbox_style
    fig.update_layout(margin={"r": 0, "t": 0, "l": 2, "b": 0})
    return fig

# Google Place API
API_KEY = ''
gmaps = googlemaps.Client(key=API_KEY)

@app.callback(
    dash.dependencies.Output('line2-fig', 'figure'),
    [dash.dependencies.Input('Place_ip', 'value')]
)
def google_place(Place_ip):
    # Convert lat & lng to distance
    from math import cos
    from math import sin
    import math

    def rad(d):
        return d * math.pi / 180.0

    def getDistance(lat1, lng1, lat2, lng2):
        EARTH_REDIUS = 6378.137
        radLat1 = rad(lat1)
        radLat2 = rad(lat2)
        a = radLat1 - radLat2
        b = rad(lng1) - rad(lng2)
        s = 2 * math.asin(math.sqrt(math.pow(sin(a / 2), 2) + cos(radLat1) * cos(radLat2) * math.pow(sin(b / 2), 2)))
        s = s * EARTH_REDIUS
        return s

    # Obtain lat & lng from input
    geocode_result = gmaps.geocode(Place_ip)

    df['lat'] = df['lat'].astype(float)
    df['lng'] = df['lng'].astype(float)

    if geocode_result:
        latG = geocode_result[0]['geometry']['location']['lat']
        lngG = geocode_result[0]['geometry']['location']['lng']
        df['dis'] = df[['lat', 'lng']].apply(lambda x: getDistance(latG, lngG, x['lat'], x['lng']), axis=1)
        dff = df[df['dis'] < 50.0]
        k = 50.0
        while len(dff) >= 10:
            dff = df[df['dis'] < k]
            k -= 0.1

        dffM_sorted = dff.sort_values(by=['user_ratings_total', 'rating', 'dis'],
                                      ascending=[False, False, True]).head()

        fig = go.Figure(data=[go.Bar(x=dffM_sorted['user_ratings_total'].astype(int).to_list(),
                                     y=list(range(1, len(dff)+1)),
                                     text=dffM_sorted['name'],
                                     orientation='h')])  # hovertext=dff_sorted['name'],
        fig.update_traces(marker_color='lightsalmon', textposition='inside')
        fig.update_layout(yaxis=dict(title="Ranking", autorange="reversed"),
                          width=620, height=480, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                          xaxis=dict(title="Total Rating Users"))

        return fig
    else:
        return 'No Result !'



@app.callback(
    dash.dependencies.Output('map2-fig', 'figure'),
    [dash.dependencies.Input('Place_ip', 'value')]
)

def Show_google_place(Place_ip):
    # Convert lat & lng to distance
    from math import cos
    from math import sin
    import math

    def rad(d):
        return d * math.pi / 180.0

    def getDistance(lat1, lng1, lat2, lng2):
        EARTH_REDIUS = 6378.137
        radLat1 = rad(lat1)
        radLat2 = rad(lat2)
        a = radLat1 - radLat2
        b = rad(lng1) - rad(lng2)
        s = 2 * math.asin(
            math.sqrt(math.pow(sin(a / 2), 2) + cos(radLat1) * cos(radLat2) * math.pow(sin(b / 2), 2)))
        s = s * EARTH_REDIUS
        return s

    # Obtain lat & lng from input
    geocode_result = gmaps.geocode(Place_ip)

    df['lat'] = df['lat'].astype(float)
    df['lng'] = df['lng'].astype(float)

    if geocode_result:
        latG = geocode_result[0]['geometry']['location']['lat']
        lngG = geocode_result[0]['geometry']['location']['lng']
        df['dis'] = df[['lat', 'lng']].apply(lambda x: getDistance(latG, lngG, x['lat'], x['lng']), axis=1)
        dff = df[df['dis'] < 50.0]
        k = 50.0
        while len(dff) >= 10:
            dff = df[df['dis'] < k]
            k -= 0.1

        dffM_sorted = dff.sort_values(by=['user_ratings_total', 'rating', 'dis'],
                                      ascending=[False, False, True]).head()

        fig = px.scatter_mapbox(dffM_sorted, lat="lat", lon="lng", hover_name="name",
                                color=dffM_sorted["rating"].astype(float).tolist(),
                                size=dffM_sorted["rating"].astype(float).tolist(),
                                color_continuous_scale=px.colors.cyclical.IceFire,
                                zoom=12, height=330)
        fig.update_layout(mapbox=dict(accesstoken=mapbox_access_token))  # mapbox_style
        fig.update_layout(margin={"r": 0, "t": 0, "l": 2, "b": 0})
        return fig
    else:
        return 'No Result !'


# ===========================================================================================

if __name__ == '__main__':
    app.run_server(debug=True)
