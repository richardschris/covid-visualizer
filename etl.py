import pandas as pd
import psycopg2
import numpy as np


cases = pd.read_csv('../COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')
deaths = pd.read_csv('../COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
recovered = pd.read_csv('../COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv')
# dates start here
dates = cases.columns[4:]

# the dataset includes some county-level data we need to aggregate into the state level
COUNTY_SUFFIXES = {
    'HI': 'Hawaii',
    'OR': 'Oregon',
    'OK': 'Oklahoma',
    'SD': 'South Dakota',
    'PA': 'Pennsylvania',
    'GA': 'Georgia',
    'IN': 'Indiana',
    'NM': 'New Mexico',
    'SC': 'South Carolina',
    'MN': 'Minnesota',
    'IA': 'Iowa',
    'CA': 'California',
    'WI': 'Wisconsin',
    'UT': 'Utah',
    'CT': 'Connecticut',
    'MI': 'Michigan',
    'KY': 'Kentucky',
    'OH': 'Ohio',
    'KS': 'Kansas',
    'MD': 'Maryland',
    'CO': 'Colorado',
    'TX': 'Texas',
    'NH': 'New Hampshire',
    'VT': 'Vermont',
    'FL': 'Florida',
    'DE': 'Delaware',
    'LA': 'Louisiana',
    'NE': 'Nebraska',
    'AZ': 'Arizona',
    'TN': 'Tennessee',
    'VA': 'Virginia',
    'IL': 'Illinois',
    'RI': 'Rhode Island',
    'MO': 'Missouri',
    'WA': 'Washington',
    'NV': 'Nevada',
    'NC': 'North Carolina',
    'MA': 'Massachusetts',
    'NY': 'New York',
    'NJ': 'New Jersey'
}

CREATE_COUNTRY_TABLE = '''
CREATE TABLE IF NOT EXISTS country (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    UNIQUE(name)
)
'''
CREATE_COUNTRY_INDEX = '''
CREATE INDEX IF NOT EXISTS country_idx ON country (name);
'''
CREATE_SUBDIVISION_TABLE = '''
CREATE TABLE IF NOT EXISTS subdivision (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    country INT REFERENCES country(id),
    UNIQUE(name, country)
)
'''
CREATE_SUBDIVISION_INDEX = '''
CREATE INDEX IF NOT EXISTS subdivision_idx ON subdivision (name);
'''
CREATE_CASES_TABLE = '''
CREATE TABLE IF NOT EXISTS cases (
    day DATE, 
    country INT REFERENCES country(id), 
    subdivision INT REFERENCES subdivision NULL, 
    positive_cases INT, 
    deaths INT,
    recovered INT,
    UNIQUE (day, country, subdivision)
);
'''
CREATE_CASES_INDEX = '''
CREATE INDEX IF NOT EXISTS cases_idx ON cases (day)
'''

INSERT_COUNTRY = '''
INSERT INTO country (name) VALUES (%s) ON CONFLICT DO NOTHING;
'''
INSERT_DUMMY_COUNTRY = '''
INSERT INTO country (id, name) VALUES (0, null) ON CONFLICT DO NOTHING;
'''
INSERT_SUBDIVISION = '''
INSERT INTO subdivision (name, country) VALUES (%s, %s) ON CONFLICT DO NOTHING;
'''
INSERT_DUMMY_SUBDIVISION = 'INSERT INTO subdivision (id, name, country) VALUES (0, null, 0) ON CONFLICT DO NOTHING'
GET_COUNTRY = '''
SELECT * FROM country WHERE name = %s;
'''
GET_SUBDIVISION = '''
SELECT * FROM subdivision WHERE name = %s;
'''
INSERT_CASES = '''
INSERT INTO cases (day, country, subdivision, positive_cases, deaths, recovered) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
'''
INCREMENT_CASES = '''
INSERT INTO cases 
    (day, country, subdivision, positive_cases, deaths, recovered) VALUES (%s, %s, %s, %s, %s, %s) 
    ON CONFLICT (day, country, subdivision) DO UPDATE SET 
        positive_cases = cases.positive_cases + EXCLUDED.positive_cases,
        deaths = cases.deaths + EXCLUDED.deaths,
        recovered = cases.recovered + EXCLUDED.recovered;
'''
TRACKED_DATES_TABLE = '''
CREATE TABLE IF NOT EXISTS tracked_dates (
    id SERIAL PRIMARY KEY,
    day DATE,
    UNIQUE (day)
);
'''
INSERT_TRACKED_DATE = '''
INSERT INTO tracked_dates (day) VALUES (%s) ON CONFLICT DO NOTHING;
'''

conn = psycopg2.connect(database='covid')
cur = conn.cursor()

cur.execute(CREATE_COUNTRY_TABLE)
cur.execute(CREATE_SUBDIVISION_TABLE)
cur.execute(CREATE_SUBDIVISION_INDEX)
cur.execute(CREATE_COUNTRY_INDEX)
cur.execute(INSERT_DUMMY_COUNTRY)
cur.execute(INSERT_DUMMY_SUBDIVISION)
cur.execute(CREATE_CASES_TABLE)
cur.execute(CREATE_CASES_INDEX)
cur.execute(TRACKED_DATES_TABLE)
conn.commit()


inconsistent_recovered_subdivision_data_countries = set()


# on 3/18 there were a bunch of NaN values
def nan_to_int(val):
    if val is np.nan or isinstance(val, float):
        return 0
    else:
        return int(val)


for _, row in cases.iterrows():
    subdivision = row['Province/State']
    country = row['Country/Region']
    increment_counter = False
    # fucking pandas
    if not isinstance(subdivision, float):

        deaths_df = deaths.loc[(deaths['Province/State'] == subdivision) & (deaths['Country/Region'] == country)]
        recovered_df = recovered.loc[(recovered['Province/State'] == subdivision) & (recovered['Country/Region'] == country)]
        cur.execute(INSERT_COUNTRY, (country,))
        subdivision = subdivision.strip()

        conn.commit()
        if subdivision[-2:] in COUNTY_SUFFIXES:
            sd = subdivision[-2:]
            subdivision = COUNTY_SUFFIXES[sd]
            increment_counter = True
        cur.execute(GET_COUNTRY, [country])
        country_id, _ = cur.fetchone()
        if not increment_counter:
            cur.execute(INSERT_SUBDIVISION, (subdivision, country_id))
        conn.commit()
        cur.execute(GET_SUBDIVISION, [subdivision])
        subdivision_id, _, _ = cur.fetchone()
    else:
        deaths_df = deaths.loc[(deaths['Province/State'].isnull()) & (deaths['Country/Region'] == country)]
        recovered_df = recovered.loc[(recovered['Province/State'].isnull()) & (recovered['Country/Region'] == country)]
        cur.execute(INSERT_COUNTRY, (country,))
        conn.commit()
        cur.execute(GET_COUNTRY, [country])
        country_id, _ = cur.fetchone()

        subdivision_id = 0  # dummy subdivision for unique constraint

    for date in dates:
        exclude_recoveries = False
        cur.execute('SELECT EXISTS (SELECT 1 FROM tracked_dates WHERE day=%s)', [date])
        is_date_tracked = cur.fetchone()[0]
        if is_date_tracked:
            continue
        cases_date = row[date]
        deaths_date = deaths_df.iloc[0][date]
        try:
            # some countries don't have subdivision recoveries data -- we ignore these countries for now
            recovered_date = recovered_df.iloc[0][date]
        except:            
            exclude_recoveries = True
        if cases_date is np.nan:
            continue  # no data for this day (yet?) -- allowing other numbers to be nan, but not this one
        if not increment_counter:
            cur.execute(
                INSERT_CASES, (
                    date, 
                    country_id, 
                    subdivision_id, 
                    nan_to_int(cases_date), 
                    nan_to_int(deaths_date), 
                    nan_to_int(recovered_date) if not exclude_recoveries else 0
                )
            )
        else:
            cur.execute(
                INCREMENT_CASES, (
                    date, 
                    country_id, 
                    subdivision_id, 
                    nan_to_int(cases_date), 
                    nan_to_int(deaths_date), 
                    nan_to_int(recovered_date) if not exclude_recoveries else 0
                )
            )
        conn.commit()

for date in dates:
    cur.execute(INSERT_TRACKED_DATE, [date])
    conn.commit()