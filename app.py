import dash
import dash_core_components as dcc
import dash_html_components as html
import psycopg2

conn = psycopg2.connect(database='covid')
cur = conn.cursor()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

DEFAULT_COUNTRY = '''SELECT id FROM country WHERE name='US';'''

COUNTRIES = '''
SELECT id, name FROM country WHERE id > 0 ORDER BY name;
'''

PROVINCES = '''
SELECT DISTINCT on (province) province, id FROM subdivision WHERE id > 0 AND country = %s ORDER BY province;
'''

SUBDIVISIONS = '''
select name, id from subdivision where province = (select province from subdivision where id = %s)
and name <> '';
'''

SELECT_BY_SUBDIVISION_DATA = '''
SELECT day, positive_cases, deaths, recovered
    FROM cases WHERE subdivision=%s
    ORDER BY day ASC;
'''

SELECT_BY_COUNTRY_DATA = '''
SELECT day, sum(positive_cases) as cases, sum(deaths) as deaths, sum(recovered) as recovered
    FROM cases a INNER JOIN country b on a.country = b.id WHERE b.id = %s
    GROUP BY day
    ORDER BY day;
'''

def get_default_country():
    cur.execute(DEFAULT_COUNTRY)
    return cur.fetchone()[0]

def get_country_data(country=99):
    cur.execute(SELECT_BY_COUNTRY_DATA, [country])
    cases = cur.fetchall()
    data = [
        {
            'cases': cases_day, 
            'deaths': deaths_day, 
            'recovered': recovered_day,
            'day': day
        } for day, cases_day, deaths_day, recovered_day in cases
    ]
    return data


def get_provinces(country=100):
    empty_choice = [('None', 0)]
    cur.execute(PROVINCES, [country])
    province = cur.fetchall()
    print(f'Getting All Provinces for country {country}')
    if not province:
        province = []
    
    empty_choice.extend(province)
    foo = [{'label': label, 'value': value} for label, value in empty_choice]
    return foo

def get_subdivisions(province = 0):
    empty_choice = [('None', 0)]
    cur.execute(SUBDIVISIONS, [province])
    subdivisions = cur.fetchall()
    print(f'Getting All Subdivisions for province {province}')
    if not subdivisions:
        subdivisions = []
    
    empty_choice.extend(subdivisions)
    foo = [{'label': label, 'value': value} for label, value in empty_choice]
    return foo


def get_subdivision_data(subdivision):
    print(f'Getting Subdivision Data {subdivision}')
    cur.execute(SELECT_BY_SUBDIVISION_DATA, [subdivision])
    cases = cur.fetchall()
    data = [
        {
            'cases': cases_day, 
            'deaths': deaths_day, 
            'recovered': recovered_day,
            'day': day, 
        }
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
                {'x': x, 'y': recovered_y, 'type': 'line', 'name': 'Recovered'}
            ],
        }


def get_countries():
    cur.execute(COUNTRIES)
    return [
        {'label': label, 'value': value} for value, label in cur.fetchall()
    ]
     


footer = [
    'Data from ', 
    html.A(children='Johns Hopkins', href="https://github.com/CSSEGISandData/COVID-19"), 
    '. Code ',
    html.A(children='here.', href="https://github.com/richardschris/covid-visualizer"),
    ' Email me at the address in my github with questions/comments/concerns/complaints/kudos. Built with ',
    html.A(href='https://plot.ly/dash/', children='Dash.')
]

data = get_country_data()
graph_data = populate_graph_data(data)
app.title = 'COVID-19 Charts'

app.layout = html.Div(children=[
    html.H1(children='COVID-19 Cases'),
    dcc.Dropdown(
        id='country-dropdown',
        options=get_countries(),
        value=get_default_country()
    ),
    dcc.Dropdown(
        id='province-dropdown',
        options=get_provinces(),
        value=None
    ),
    dcc.Dropdown(
        id='subdivision-dropdown',
        options=get_subdivisions(),
        value=None
    ),
    dcc.Graph(
        id='covid-graph',
        figure=graph_data
    ),
    html.Footer(children=footer)
])


@app.callback(
    dash.dependencies.Output('covid-graph', 'figure'),
    [
        dash.dependencies.Input('country-dropdown', 'value'),
        dash.dependencies.Input('province-dropdown', 'value'),
        dash.dependencies.Input('subdivision-dropdown', 'value')
    ]
)
def update_graph(country=100, province=None, subdivision=None):
    if subdivision:
        data = get_subdivision_data(subdivision)
    else:
        data = get_country_data(country)
    graph_data = populate_graph_data(data)
    return graph_data


@app.callback(
    dash.dependencies.Output('province-dropdown', 'options'),
    [
        dash.dependencies.Input('country-dropdown', 'value')
    ]
)
def update_provinces_dropdown(country=100):
    values = get_provinces(country)
    return values


@app.callback(
    dash.dependencies.Output('province-dropdown', 'value'),
    [
        dash.dependencies.Input('country-dropdown', 'value')
    ]
)
def reset_provinces_dropdown(*args, **kwargs):
    return None


@app.callback(
    dash.dependencies.Output('subdivision-dropdown', 'options'),
    [
        dash.dependencies.Input('province-dropdown', 'value')
    ]
)
def update_subdivisions_dropdown(subdivision = 0):
    values = get_subdivisions(subdivision)
    return values

@app.callback(
    dash.dependencies.Output('subdivision-dropdown', 'value'),
    [
        dash.dependencies.Input('province-dropdown', 'value')
    ]
)
def reset_subdivisions_dropdown(*args, **kwargs):
    return None

if __name__ == '__main__':
    app.run_server(debug=True)
