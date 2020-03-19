import pandas as pd
import psycopg2
import numpy as np

conn = psycopg2.connect(database='covid')
cur = conn.cursor()

cases = pd.read_csv('../COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv')
deaths = pd.read_csv('../COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv')
recovered = pd.read_csv('../COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv')
# dates start here
dates = cases.columns[4:]

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

cur.execute(CREATE_COUNTRY_TABLE)
cur.execute(CREATE_SUBDIVISION_TABLE)
cur.execute(CREATE_SUBDIVISION_INDEX)
cur.execute(CREATE_COUNTRY_INDEX)
cur.execute(INSERT_DUMMY_COUNTRY)
cur.execute(INSERT_DUMMY_SUBDIVISION)
cur.execute(CREATE_CASES_TABLE)
cur.execute(CREATE_CASES_INDEX)
conn.commit()


# on 3/18 there were a bunch of NaN values
def nan_to_int(val):
    if val is np.nan:
        return 0
    else:
        return int(val)

for _, row in cases.iterrows():
    subdivision = row['Province/State']
    country = row['Country/Region']

    # fucking pandas
    if not isinstance(subdivision, float):
        deaths_df = deaths.loc[(deaths['Province/State'] == subdivision) & (deaths['Country/Region'] == country)]
        recovered_df = recovered.loc[(recovered['Province/State'] == subdivision) & (recovered['Country/Region'] == country)]
        cur.execute(INSERT_COUNTRY, (country,))
        conn.commit()
        cur.execute(GET_COUNTRY, [country])
        country_id, _ = cur.fetchone()
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
        cases_date = row[date]
        deaths_date = deaths_df.iloc[0][date]
        recovered_date = recovered_df.iloc[0][date]
        if cases_date is np.nan:
            continue  # no data for this day (yet?) -- allowing other numbers to be nan, but not this one
        cur.execute(INSERT_CASES, (date, country_id, subdivision_id, nan_to_int(cases_date), nan_to_int(deaths_date), nan_to_int(recovered_date)))
        conn.commit()
