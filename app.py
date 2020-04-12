import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_daq
import psycopg2
from psycopg2.extensions import AsIs


conn = psycopg2.connect(database='covid')
cur = conn.cursor()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

DEFAULT_COUNTRY = '''SELECT id FROM country WHERE name='US';'''

COUNTRIES = '''
SELECT id, name FROM country WHERE id > 0 ORDER BY name;
'''
SUBDIVISIONS = '''
SELECT DISTINCT id, name FROM subdivision WHERE id > 0 AND country = %s ORDER BY name;
'''
COUNTIES = '''
SELECT id, name FROM county where ID > 0 AND subdivision = %s ORDER BY name;
'''

SELECT_BY_COUNTY_DATA = '''
SELECT day, positive_cases, deaths, recovered FROM cases WHERE county=%s ORDER BY day;
'''

SELECT_BY_SUBDIVISION_DATA = '''
SELECT day, sum(positive_cases), sum(deaths), sum(recovered)
    FROM cases WHERE subdivision=%s
    GROUP BY day
    ORDER BY day ASC;
'''

SELECT_BY_COUNTRY_DATA = '''
SELECT day, sum(positive_cases) as cases, sum(deaths) as deaths, sum(recovered) as recovered
    FROM cases a INNER JOIN country b on a.country = b.id WHERE b.id = %s
    GROUP BY day
    ORDER BY day;
'''

SELECT_WORLD_DATA = '''
SELECT day, sum(positive_cases) as cases, sum(deaths) as deaths, sum(recovered) as recovered
    FROM cases
    GROUP BY day
    ORDER BY day;
'''

SELECT_ROLLING_AVERAGE = '''
SELECT day, sum(cases) AS cases, sum(deaths) AS deaths, sum(recovered) AS recovered
    FROM %(table)s
    WHERE ref_id=%(ref_id)s
    GROUP BY day
    ORDER BY day;
'''

def get_default_country():
    cur.execute(DEFAULT_COUNTRY)
    return cur.fetchone()[0]


def prepare_graph(cases):
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


def get_rolling_average_data(table_type='country', ref_id=None):
    tables = {
        'country': 'view_moving_averages_country',
        'subdivision': 'view_moving_averages_subdivision',
        'county': 'view_moving_averages_county',
    }
    table_name = tables[table_type]
    cur.execute(SELECT_ROLLING_AVERAGE, {'table': AsIs(table_name), 'ref_id': ref_id})


def get_country_data(country, plot_type):
    if country == 0:
        cur.execute(SELECT_WORLD_DATA)
    elif plot_type != 'moving-average':
        cur.execute(SELECT_BY_COUNTRY_DATA, [country])
    else:
        get_rolling_average_data(table_type='country', ref_id=country)
    cases = cur.fetchall()
    return prepare_graph(cases)


def get_subdivisions(country=100):
    choices = [(0, 'None')]
    cur.execute(SUBDIVISIONS, [country])
    subdivisions = cur.fetchall()
    if not subdivisions:
        subdivisions = []
    
    choices.extend(subdivisions)
    return [{'label': label, 'value': value} for value, label in choices]


def get_counties(subdivision=None):
    choices = [(0, 'None')]
    cur.execute(COUNTIES, [subdivision])
    counties = cur.fetchall()
    if not counties:
        counties = []

    choices.extend(counties)
    return [{'label': label, 'value': value} for value, label in choices]


def get_subdivision_data(subdivision, plot_type):
    if plot_type != 'moving-average':
        cur.execute(SELECT_BY_SUBDIVISION_DATA, [subdivision])
    else:
        get_rolling_average_data(table_type='subdivision', ref_id=subdivision)
    cases = cur.fetchall()
    return prepare_graph(cases)


def get_county_data(county, plot_type):
    if plot_type != 'moving-average':
        cur.execute(SELECT_BY_COUNTY_DATA, [county])
    else:
        get_rolling_average_data(table_type='county', ref_id=county)
    cases = cur.fetchall()
    return prepare_graph(cases)


def populate_graph_data(data, axis_type):
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
        'layout': {
            'yaxis': {'type': axis_type}
        }
    }


def get_countries():
    cur.execute(COUNTRIES)
    countries = [
        {'label': label, 'value': value} for value, label in cur.fetchall()
    ]
    countries.extend([{'label': 'World', 'value': 0}])
    return countries
     

footer = [
    'Data from ', 
    html.A(children='Johns Hopkins', href="https://github.com/CSSEGISandData/COVID-19"), 
    '. Code ',
    html.A(children='here.', href="https://github.com/richardschris/covid-visualizer"),
    ' Email me at the address in my github with questions/comments/concerns/complaints/kudos. Built with ',
    html.A(href='https://plot.ly/dash/', children='Dash.')
]

data = get_country_data(get_default_country(), 'linear')
graph_data = populate_graph_data(data, 'linear')
app.title = 'COVID-19 Charts'

plot_types = [
    {'value': 'linear', 'label': 'Linear'},
    {'value': 'log', 'label': 'Log'},
    {'value': 'moving-average', 'label': '3-Day Moving Average'}
]

app.layout = html.Div(children=[
    html.H1(children='COVID-19 Cases'),
    dcc.Dropdown(
        id='country-dropdown',
        options=get_countries(),
        value=get_default_country()
    ),
    dcc.Dropdown(
        id='subdivision-dropdown',
        options=get_subdivisions(),
        value=None,
        placeholder='Province/State (if applicable)'
    ),
    dcc.Dropdown(
        id='county-dropdown',
        options=get_counties(),
        value=None,
        placeholder='County (US only)'
    ),
    dcc.Dropdown(
        id='plot-type-button',
        options=plot_types,
        value='linear',
        placeholder='Plot Type'
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
        dash.dependencies.Input('subdivision-dropdown', 'value'),
        dash.dependencies.Input('county-dropdown', 'value'),
        dash.dependencies.Input('plot-type-button', 'value')
    ]
)
def update_graph(country=None, subdivision=None, county=None, plot_type='linear'):
    if subdivision:
        if county:
            data = get_county_data(county, plot_type)
        else:
            data = get_subdivision_data(subdivision, plot_type)
    else:
        data = get_country_data(country, plot_type)

    axis_types = {
        'linear': 'linear',
        'log': 'log',
        'moving-average': 'linear'
    }
    graph_data = populate_graph_data(data, axis_type=axis_types[plot_type])
    return graph_data


@app.callback(
    dash.dependencies.Output('subdivision-dropdown', 'options'),
    [
        dash.dependencies.Input('country-dropdown', 'value')
    ]
)
def update_state_dropdown(country=100):
    values = get_subdivisions(country)
    return values


@app.callback(
    dash.dependencies.Output('county-dropdown', 'options'),
    [
        dash.dependencies.Input('subdivision-dropdown', 'value')
    ]
)
def update_county_dropdown(county=0):
    values = get_counties(county)
    return values


@app.callback(
    dash.dependencies.Output('county-dropdown', 'value'),
    [
        dash.dependencies.Input('subdivision-dropdown', 'value')
    ]
)
def reset_county_dropdown(*args, **kwargs):
    return None


@app.callback(
    dash.dependencies.Output('subdivision-dropdown', 'value'),
    [
        dash.dependencies.Input('country-dropdown', 'value')
    ]
)
def reset_state_dropdown(*args, **kwargs):
    return None


if __name__ == '__main__':
    app.run_server(debug=True)
