# import packages
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State, MATCH, ALL
from dash.exceptions import PreventUpdate
import dash_daq as daq
import dash_bootstrap_components as dbc
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

import base64
import datetime as dt
import json
import numpy as np
import os
import pandas as pd
import statsmodels.api as sm
from sqlalchemy import create_engine
import re

app = dash.Dash(
    __name__, 
    title='Treasure NFT Sales',
    meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1.0'}])

app.index_string = """<!DOCTYPE html>
<html>
    <head>
        <!-- Global site tag (gtag.js) - Google Analytics -->
        <script async src="https://www.googletagmanager.com/gtag/js?id=G-DR5F0RFK4C"></script>
        <script>
          window.dataLayer = window.dataLayer || [];
          function gtag(){dataLayer.push(arguments);}
          gtag('js', new Date());
        
          gtag('config', 'G-DR5F0RFK4C');
        </script>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>"""

application = app.server

plot_color_palette = [
    '#ff0063',
    '#8601fe',
    '#05ff9c',
    '#fefe00',
    '#1601ff',
]

# define user inputs and attributes matching
attributes_by_collection = {
    'treasures': ['nft_subcategory'],
    'smol_brains': ['gender','body','hat','glasses','mouth','clothes', 'is_one_of_one'], 
    'legions_genesis': ['nft_subcategory'],
    'smol_cars': ['background','base_color','spots','tire_color','window_color','tip_color','lights_color','door_color','wheel_color', 'is_one_of_one'],
    'life': [np.nan],
    'smol_brain_lands': [np.nan],
    'smol_bodies': ['gender','background', 'body','clothes','feet','hands','head'],
    'quest_keys': [np.nan],
    'legions': ['nft_subcategory'],
    'extra_life': [np.nan]
}

collections = list(attributes_by_collection.keys()) + ['all']


date_toggle_options = {
    '1 day': 1,
    '7 day': 7,
    '30 day': 30,
    'All time': 100000
}

date_interval_options = {
    '15 min': '15min',
    '30 min': '30min',
    '1 hour': '1h',
    '6 hour': '6h',
    '12 hour': '12h',
    '1 day': '1d'
}

pricing_unit_options = {
    'MAGIC': 'sale_amt_magic',
    'USD': 'sale_amt_usd',
    'ETH': 'sale_amt_eth'
}

dropdown_style = {
    'color':'#FFFFFF',
    'background-color':'#374251', 
    'border-color':'rgb(229 231 235)', 
    'border-radius':'0.375rem',
    'white-space': 'nowrap'
}

# connect to database
def db_connect():
    sql_credential = os.path.join("static", "mysql_credential.json")
    with open(sql_credential) as f:
        mysql_credentials = json.loads(f.read())
    engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}/{db}".format(
        user=mysql_credentials['username'], 
        pw=mysql_credentials['pw'], 
        host=mysql_credentials['host'], 
        db="treasure"
        )
    )
    connection = engine.connect()
    return connection

# define functions
def q75(x):
    return np.percentile(x, 75)

def q25(x):
    return np.percentile(x, 25)

def get_sales(collection, lookback_window, display_currency, connection):
    # read in sales data
    min_datetime = dt.datetime.now() - dt.timedelta(days = lookback_window)
    sales_query  = 'SELECT tx_hash, datetime, sale_amt_magic, nft_collection, nft_id, nft_subcategory, quantity FROM treasure.marketplace_sales WHERE datetime >= %(min_datetime)s'
    if collection != 'all':
        sales_query = sales_query + 'AND nft_collection = %(collection)s'
    marketplace_sales_list = []
    marketplace_sales_query = connection.execute(sales_query, {'min_datetime': min_datetime, 'collection': collection})
    for row in marketplace_sales_query:
        marketplace_sales_list.append(row)
    marketplace_sales = pd.DataFrame(marketplace_sales_list)
    if len(marketplace_sales)==0:
        return pd.DataFrame()
    marketplace_sales.columns=list(marketplace_sales_query.keys())
    marketplace_sales['date'] = marketplace_sales['datetime'].dt.date

    # split out multiple-quantity sales into individual rows
    multi_sales = marketplace_sales.loc[marketplace_sales['quantity']>1].copy()
    marketplace_sales = marketplace_sales.loc[marketplace_sales['quantity']==1].copy()
    multi_sales['sale_amt_magic'] = multi_sales['sale_amt_magic'] / multi_sales['quantity']
    multi_sales = multi_sales.loc[multi_sales.index.repeat(multi_sales['quantity'])].reset_index(drop=True)
    multi_sales['quantity'] = 1
    marketplace_sales = pd.concat([marketplace_sales, multi_sales])

    # read in token prices
    if display_currency != 'MAGIC':
        prices_query = 'SELECT * FROM treasure.token_prices WHERE datetime >= %(min_datetime)s'
        token_prices_list = []
        token_prices_query = connection.execute(prices_query, {'min_datetime': min_datetime})
        for row in token_prices_query:
            token_prices_list.append(row)
        token_prices = pd.DataFrame(token_prices_list)
        token_prices.columns=list(token_prices_query.keys())

        token_prices['date'] = token_prices['datetime'].dt.date
        token_prices.rename(columns={'datetime':'token_price_datetime'}, inplace=True)

        marketplace_sales = marketplace_sales.merge(token_prices, how='left', on='date')
        marketplace_sales['token_price_sale_datetime_diff'] = marketplace_sales['datetime'] - marketplace_sales['token_price_datetime']
        most_recent_token_prices = marketplace_sales.groupby('tx_hash',as_index=False).agg({'token_price_sale_datetime_diff':'min'})
        marketplace_sales = marketplace_sales.merge(most_recent_token_prices, how='inner', on=['tx_hash', 'token_price_sale_datetime_diff'])
        marketplace_sales['sale_amt_usd'] = marketplace_sales['sale_amt_magic'] * marketplace_sales['price_magic_usd']
        marketplace_sales['sale_amt_eth'] = (marketplace_sales['sale_amt_magic'] * marketplace_sales['price_magic_usd']) / marketplace_sales['price_eth_usd']

    return marketplace_sales

# initialize data and attributes
connection = db_connect()

marketplace_sales = get_sales('all', 7, 'MAGIC', connection)

attributes_dfs = {}
for key, value in attributes_by_collection.items():
    if (pd.isnull(value[0])):
        continue
    elif (len(value) > 1):
        tmp_attributes_lst = []
        tmp_attributes_query = connection.execute('SELECT * FROM treasure.attributes_{}'.format(key))
        for row in tmp_attributes_query:
            tmp_attributes_lst.append(row)
        tmp_attributes = pd.DataFrame(tmp_attributes_lst)
        tmp_attributes.columns=list(tmp_attributes_query.keys())
        tmp_attributes = tmp_attributes.loc[:, value + ['id']]
        attributes_dfs[key] = tmp_attributes
    else:
        tmp_attributes = marketplace_sales.loc[marketplace_sales['nft_collection']==key, value + ['nft_id']].drop_duplicates()
        tmp_attributes.rename(columns={'nft_id':'id'}, inplace=True)
        attributes_dfs[key] = tmp_attributes

connection.close()

# create app layout
app.layout = html.Div([
    dcc.Location(id='url-input', refresh=False),
    dcc.Location(id='url-output', refresh=False),
    html.Div([
        html.Img(id='treasureLogo', src=app.get_asset_url('img/treasure_logo.png')),
        html.H1('Treasure NFT Sales', id='bannerTitle')
        ], className='bannerContainer'),
    html.Div([
        html.Div([
            html.Div([
                html.Div('NFT Collection:', className='headlineControlText'),
                dcc.Dropdown(
                    id='collection_dropdown',
                    options=[{'label': i.title().replace('_', ' '), 'value': i} for i in collections],
                    value='all',
                    clearable=False,
                    style=dropdown_style,
                    className='headlineDropdown')], 
                className='headlineControl'),
            html.Div([
                html.Div('Display Currency:', className='headlineControlText'),
                dcc.Dropdown(
                    id='pricing_unit',
                    options=[{'label': key, 'value': value} for key, value in pricing_unit_options.items()],
                    value='sale_amt_magic',
                    clearable=False,
                    style=dropdown_style,
                    className='headlineDropdown')],
                className='headlineControl'),
            html.Div([
                html.Div('Lookback Window:', className='headlineControlText'),
                dcc.Dropdown(
                    id='time_window',
                    options=[{'label': key, 'value': value} for key, value in date_toggle_options.items()],
                    value=7,
                    clearable=False,
                    style=dropdown_style,
                    className='headlineDropdown')],
                className='headlineControl')],
                id='headlineControlContainer'),
        html.Div(id='attributeDropdownContainer', children=[])], id='controls'),
    html.Div([
            html.Div([html.Div('Number of Sales: ', className='summaryStatLabel'), html.Div(id='n_sales', className='summaryStatMetric')], className='summaryStatBox'),
            html.Div([html.Div('Min Sale Price: ', className='summaryStatLabel'), html.Div(id='min_sale', className='summaryStatMetric')], className='summaryStatBox'),
            html.Div([html.Div('Avg Sale Price: ', className='summaryStatLabel'), html.Div(id='avg_sale', className='summaryStatMetric')], className='summaryStatBox'),
            html.Div([html.Div('Total Volume: ', className='summaryStatLabel'), html.Div(id='volume', className='summaryStatMetric')], className='summaryStatBox')],
        id='summaryStatsContainer'),
    html.Div([
        html.Div('Outliers'),
        daq.ToggleSwitch(
            id='outlier_toggle',
            label=['Show', 'Hide'],
            color='#374251',
            value=True
        ),
    ], id='outlierToggleContainer'),
    dcc.Graph(id='sales_scatter'),
    html.Div([
        dbc.Col('Frequency:'),
        dbc.Col(dcc.Dropdown(
            id='time_interval',
            options=[{'label': key, 'value': value} for key, value in date_interval_options.items()],
            value='1d',
            clearable=False,
            style=dropdown_style
        ))
    ], id='frequencyIntervalContainer'),
    dcc.Graph(id='volume_floor_prices'),
])

# function to dynamically update attribute inputs based on the collection
@app.callback(
    Output('attributeDropdownContainer', 'children'),
    Input('collection_dropdown', 'value'),
    State('attributeDropdownContainer', 'children'))
def display_dropdowns(collection_value, children):
    if collection_value=='all':
        id_columns = [np.nan]
    else:
        id_columns = attributes_by_collection[collection_value]
        if 'is_one_of_one' in id_columns:
            id_columns.remove('is_one_of_one')
    children = []
    if (not pd.isnull(id_columns[0])): 
        attributes_df = attributes_dfs[collection_value]
        attributes_df = attributes_df.fillna('N/A')
        for attribute in id_columns:
            new_dropdown = html.Div([
                html.Div(
                    id={
                        'type':'filter_label',
                        'index':attribute
                    },
                    className='attributeText'
                ),
                dcc.Dropdown(
                    id={
                        'type':'filter_dropdown',
                        'index':attribute
                    },
                    options=[{'label': i, 'value': i} for i in list(attributes_df[attribute].unique()) + ['any']],
                    value='any',
                    clearable=False,
                    style=dropdown_style
                )
            ], className='attributeBox')
            children.append(new_dropdown)
        button = html.Div([
            html.Div('blank', id='buttonLabel'),
            html.Div(html.Button('Reset',id='attributeResetButton', n_clicks=0, style=dropdown_style))
        ], id = 'attributeResetContainer')
        children.append(button)
    return children

# function to reset attribute values
@app.callback(
    Output({'type': 'filter_dropdown', 'index': ALL}, 'value'),
    Input('attributeResetButton', 'n_clicks'),
    State({'type': 'filter_dropdown', 'index': ALL}, 'id'),
    Input('url-input', 'pathname')
)
def reset_attributes(reset, id_value, url_path):
    ctx = dash.callback_context

    reset_val_list = []
    for id in id_value:
        if ctx.triggered[0]['prop_id']=='attributeResetButton.n_clicks':
            reset_val_list.append('any')
        else:
            reset_val_list.append(re.findall("(?<={}=)[a-zA-Z10-9%/_-]+".format(id['index']), url_path)[0].replace('%20', ' '))
    return reset_val_list

# function to update the attribute labels
@app.callback(
    Output({'type': 'filter_label', 'index': MATCH}, 'children'),
    Input({'type': 'filter_dropdown', 'index': MATCH}, 'id'),
)
def display_output(id):
    title = id['index'].replace('_', ' ')
    if title == 'nft subcategory':
        title = 'Type' 
    return html.Div('{}:'.format(title))

# function to dynamically update inputs for brains and bodies based on gender
@app.callback(
    Output({'type': 'filter_dropdown', 'index': ALL}, 'options'),
    Input({'type': 'filter_dropdown', 'index': ALL}, 'value'),
    State('collection_dropdown', 'value'),
    State({'type': 'filter_dropdown', 'index': ALL}, 'id'),
)
def filter_attributes_gender(filter_value, collection_value, filter_id):
    if 'gender' not in  attributes_by_collection[collection_value]:
        raise PreventUpdate
    if 'male' in filter_value:
        gender_value = 'male'
    elif 'female' in filter_value:
        gender_value = 'female'
    else:
        gender_value = 'any'
    attributes_df = attributes_dfs[collection_value]
    attributes_df = attributes_df.fillna('N/A')
    attributes_df_filtered = attributes_df.loc[attributes_df['gender'].isin([gender_value] if gender_value!='any' else attributes_df['gender'].unique())].copy()

    options = []
    for item in filter_id:
        if item['index']=='gender':
            options.append([{'label': i, 'value': i} for i in list(attributes_df[item['index']].unique()) + ['any']])
        else:
            options.append([{'label': i, 'value': i} for i in list(attributes_df_filtered[item['index']].unique()) + ['any']])
    return options

# callback to filter data based on inputs
@app.callback(
    Output('n_sales', 'children'),
    Output('min_sale', 'children'),
    Output('avg_sale', 'children'),
    Output('volume', 'children'),
    Output('sales_scatter', 'figure'),
    Output('volume_floor_prices', 'figure'),
    Output('url-output', 'pathname'),
    Input('collection_dropdown', 'value'),
    Input({'type': 'filter_dropdown', 'index': ALL}, 'value'),
    State({'type': 'filter_dropdown', 'index': ALL}, 'id'),
    Input('pricing_unit', 'value'),
    Input('time_window', 'value'),
    Input('outlier_toggle', 'value'),
    Input('time_interval', 'value'),
    )
def update_stats(collection_value, value_columns, filter_columns, pricing_unit_value, time_window_value, outlier_toggle_value, time_interval_value):
    connection = db_connect()

    pricing_unit_label = 'MAGIC'
    if pricing_unit_value == 'sale_amt_usd':
        pricing_unit_label = 'USD'
    if pricing_unit_value == 'sale_amt_eth':
        pricing_unit_label = 'ETH'

    new_url = "/collection={}".format(collection_value)
    
    marketplace_sales_filtered = get_sales(collection_value, time_window_value, pricing_unit_label, connection)
    if len(marketplace_sales_filtered)==0:
        marketplace_sales_filtered = marketplace_sales.loc[marketplace_sales['nft_collection']=='000000'] # cruft to allow us to take the column names
        marketplace_sales_filtered['sale_amt_eth'] = np.nan
        marketplace_sales_filtered['sale_amt_usd'] = np.nan
    if collection_value=='all':
        id_columns = [np.nan]
    else:
        id_columns = attributes_by_collection[collection_value]
        marketplace_sales_filtered = marketplace_sales_filtered.loc[marketplace_sales_filtered['nft_collection']==collection_value]
    if len(id_columns) > 1:
        attributes_df = attributes_dfs[collection_value]
        attributes_df = attributes_df.fillna('N/A')
        marketplace_sales_filtered = marketplace_sales_filtered.merge(attributes_df, how='inner',left_on='nft_id', right_on='id')

    if filter_columns:
        for filt, val in zip(filter_columns, value_columns):
            marketplace_sales_filtered = marketplace_sales_filtered.loc[marketplace_sales_filtered[filt['index']].isin([val]) if val!='any' else marketplace_sales_filtered[filt['index']].isin(marketplace_sales_filtered[filt['index']].unique())]
            new_url = new_url + '&{}={}'.format(filt['index'], val)
    marketplace_sales_filtered = marketplace_sales_filtered.loc[marketplace_sales_filtered['datetime'] >= pd.to_datetime(dt.datetime.now() - dt.timedelta(days = time_window_value))]
    new_url = new_url + '&lookback={}'.format(time_window_value)

    sales = marketplace_sales_filtered[pricing_unit_value].count()
    min_price = marketplace_sales_filtered[pricing_unit_value].min()
    avg_price = marketplace_sales_filtered[pricing_unit_value].mean()
    volume = marketplace_sales_filtered[pricing_unit_value].sum()
    new_url = new_url + '&priceunit={}'.format(pricing_unit_value)


    if outlier_toggle_value:
        # use daily IQR
        outlier_calc = marketplace_sales_filtered.groupby('date', as_index=True).agg({pricing_unit_value:[q25, q75]})
        outlier_calc.columns = outlier_calc.columns.droplevel(0)
        outlier_calc = outlier_calc.rename_axis(None, axis=1)
        outlier_calc['cutoff'] = (outlier_calc['q75'] - outlier_calc['q25']) * 1.5
        outlier_calc['upper'] = outlier_calc['q75'] + outlier_calc['cutoff']
        outlier_calc['lower'] = outlier_calc['q25'] - outlier_calc['cutoff']

        marketplace_sales_filtered = marketplace_sales_filtered.merge(outlier_calc, how='inner', on='date')
        marketplace_sales_filtered = marketplace_sales_filtered.loc[marketplace_sales_filtered[pricing_unit_value] <= marketplace_sales_filtered['upper']]
        marketplace_sales_filtered = marketplace_sales_filtered.loc[marketplace_sales_filtered[pricing_unit_value] >= marketplace_sales_filtered['lower']]
        
        new_url = new_url + '&outliers=true'
    else:
        new_url = new_url + '&outliers=false'

    marketplace_sales_filtered['nft_collection_formatted'] = [x.title().replace("_", " ")  for x in marketplace_sales_filtered['nft_collection']]
    marketplace_sales_filtered.loc[pd.isnull(marketplace_sales_filtered['nft_subcategory']), 'nft_subcategory'] = ''
    fig1 = px.scatter(marketplace_sales_filtered,
                     x='datetime',
                     y=pricing_unit_value,
                    #  trendline='ols',
                     custom_data=['nft_collection_formatted', 'nft_id', 'nft_subcategory'],
                     color_discrete_sequence=plot_color_palette)
    fig1.update_traces(hovertemplate='%{customdata[0]} %{customdata[1]}<br>%{customdata[2]}<br><br>Date: %{x}<br>Sale Amount: %{y}')

    # trendline
    if len(marketplace_sales_filtered) > 1:
        Y = marketplace_sales_filtered[pricing_unit_value]
        X = pd.to_datetime(marketplace_sales_filtered['datetime']).map(dt.datetime.toordinal)
        X = sm.add_constant(X)
        model = sm.OLS(Y,X)
        regression = model.fit()
        marketplace_sales_filtered['preds'] = regression.predict(X)
        marketplace_sales_filtered.sort_values(by='datetime', inplace=True)

        fig1.add_trace(
            go.Scatter(
                x=marketplace_sales_filtered['datetime'], 
                y=marketplace_sales_filtered['preds'], 
                mode='lines', 
                hoverinfo='skip', 
                marker={'color':plot_color_palette[0]}, 
                line = {'shape':'spline', 'smoothing':1.3})
            )
    fig1.update_traces(marker=dict(size=6, line=dict(width=1, color='DarkSlateGrey')))
    fig1.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        hovermode='closest',
        showlegend=False)
    
    fig1.update_xaxes(title = '',
                     type='date',
                     gridcolor='#222938')
    fig1.update_yaxes(title='{}'.format(pricing_unit_label),
                     type='linear',
                     gridcolor='#8292a4')

    marketplace_sales_agg = marketplace_sales_filtered.copy()
    marketplace_sales_agg['datetime'] = marketplace_sales_agg['datetime'].dt.floor(time_interval_value)
    marketplace_sales_agg = marketplace_sales_agg.groupby('datetime').agg({pricing_unit_value:['sum', 'min','mean']})
    marketplace_sales_agg.columns = marketplace_sales_agg.columns.droplevel(0)
    marketplace_sales_agg = marketplace_sales_agg.rename_axis(None, axis=1)

    new_url = new_url + '&timeinterval={}'.format(time_interval_value)

    fig2 = make_subplots(specs=[[{"secondary_y": True}]])

    fig2.add_scatter(x=marketplace_sales_agg.index,
                     y=marketplace_sales_agg['mean'],
                     name='Average Sale',
                     mode='lines',
                     secondary_y=True,
                     marker={'color':plot_color_palette[0], 'line':{'width':50}})
    fig2.add_scatter(x=marketplace_sales_agg.index,
                     y=marketplace_sales_agg['min'],
                     name='Minimum Sale',
                     mode='lines',
                     secondary_y=True,
                     marker={'color':plot_color_palette[2], 'line':{'width':50}})
    fig2.add_bar(x=marketplace_sales_agg.index,
                     y=marketplace_sales_agg['sum'],
                     name='Volume',
                     marker={'color':plot_color_palette[1], 'line': {'width':1.5, 'color':'DarkSlateGrey'}})
    fig2.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        hovermode='closest',
        font_color='white',
        legend=dict(
        yanchor="bottom",
        y=-0.4,
        xanchor="left",
        x=0.85))
    fig2.update_xaxes(type='date')
    fig2.update_yaxes(title='Volume, {}'.format(pricing_unit_label),
                     type='linear',
                     gridcolor='#8292a4')
    fig2['layout']['yaxis2']['showgrid'] = False
    fig2['layout']['yaxis2']['title'] = 'Avg Sale Amount'

    connection.close()

    return '{:,.0f}'.format(sales),\
            '{:,.2f}'.format(min_price),\
            '{:,.2f}'.format(avg_price),\
            '{:,.2f}'.format(volume),\
            fig1,\
            fig2,\
            new_url

# callback to filter data based on URL
@app.callback(
    Output('collection_dropdown', 'value'),
    Output('pricing_unit', 'value'),
    Output('time_window', 'value'),
    Output('outlier_toggle', 'value'),
    Output('time_interval', 'value'),
    Input('url-input', 'pathname')
    )
def update_inputs(url_path):
    return re.findall("(?<=collection=)[a-z0-9_]+", url_path)[0],\
        re.findall("(?<=priceunit=)[a-z0-9_]+", url_path)[0],\
        int(re.findall("(?<=lookback=)[a-z0-9_]+", url_path)[0]),\
        True if re.findall("(?<=outliers=)[a-z0-9_]+", url_path)[0]=='true' else False,\
        re.findall("(?<=timeinterval=)[a-z0-9_]+", url_path)[0]

if __name__ == '__main__':
    app.run_server(debug=True, dev_tools_silence_routes_logging = False, dev_tools_props_check = False)
    # application.run(debug=False, port=8080)
