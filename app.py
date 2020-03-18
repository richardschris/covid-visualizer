import dash
import dash_core_components as dcc
import dash_html_components as html
import psycopg2

conn = psycopg2.connect(database='covid')
cur = conn.cursor()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

COUNTRIES = '''
SELECT id, name FROM country ORDER BY name;
'''
SELECT_BY_SUBDIVISION_DATA = '''
SELECT day, positive_cases, deaths 
    FROM cases a INNER JOIN subdivision b ON a.subdivision = b.id WHERE b.name = 'Hubei'
    ORDER BY day ASC;
'''
SELECT_BY_COUNTRY_DATA = '''
SELECT day, sum(positive_cases) as cases, sum(deaths) as deaths, sum(recovered) as recovered
    FROM cases a INNER JOIN country b on a.country = b.id WHERE b.id = %s
    GROUP BY day
    ORDER BY day;
'''

def get_country_data(country=99):
    cur.execute(SELECT_BY_COUNTRY_DATA, [country])
    cases = cur.fetchall()
    data = [
        {
            'cases': cases_day, 
            'deaths': deaths_day, 
            'day': day, 
            'recovered': recovered_day}
         for day, cases_day, deaths_day, recovered_day in cases
    ]
    return data

def populate_graph_data(data):
    cases_y = [day['cases'] for day in data]
    deaths_y = [day['deaths'] for day in data]
    recovered_y = [day['recovered'] for day in data]
    x = [day['day'] for day in data]
    return {
        'data': [
                {'x': x, 'y': cases_y, 'type': 'line', 'name': 'Cases'},
                {'x': x, 'y': deaths_y, 'type': 'line', 'name': 'Deaths'},
                {'x': x, 'y': recovered_y, 'type': 'line', 'name': 'Recovered'},
            ]
        }

def get_countries():
    cur.execute(COUNTRIES)
    return cur.fetchall()


data = get_country_data()
graph_data = populate_graph_data(data)

app.layout = html.Div(children=[
    html.H1(children='COVID-19 Cases'),
    dcc.Dropdown(
        id='country-dropdown',
        options=[
            {'label': label, 'value': value} for value, label in get_countries() 
        ],
        value=99
    ),
    dcc.Graph(
        id='covid-graph',
        figure=graph_data
    )
])
@app.callback(
    dash.dependencies.Output('covid-graph', 'figure'),
    [dash.dependencies.Input('country-dropdown', 'value')]
)
def update_graph(country=99):
    data = get_country_data(country)
    graph_data = populate_graph_data(data)
    return graph_data


if __name__ == '__main__':
    app.run_server(debug=True)
