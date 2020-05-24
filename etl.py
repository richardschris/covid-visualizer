import pandas as pd
import psycopg2
import numpy as np

DataFrame = pd.DataFrame

cases = pd.read_csv('../COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')
deaths = pd.read_csv('../COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
recovered = pd.read_csv('../COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv')

# dates start here
dates = cases.columns[4:]


CREATE_COUNTRY_TABLE = '''
CREATE TABLE IF NOT EXISTS country (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    UNIQUE(name)
);
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
);
'''
CREATE_COUNTY_TABLE = '''
CREATE TABLE IF NOT EXISTS county (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    subdivision INT REFERENCES subdivision(id),
    UNIQUE(name, subdivision)
);
'''
CREATE_SUBDIVISION_INDEX = '''
CREATE INDEX IF NOT EXISTS subdivision_idx ON subdivision (name);
'''
CREATE_COUNTY_INDEX = '''
CREATE INDEX IF NOT EXISTS county_idx ON county (name);
'''
CREATE_CASES_TABLE = '''
CREATE TABLE IF NOT EXISTS cases (
    day DATE, 
    country INT REFERENCES country(id), 
    subdivision INT REFERENCES subdivision NULL, 
    county INT REFERENCES county NULL,
    positive_cases INT, 
    deaths INT,
    recovered INT,
    UNIQUE (day, country, subdivision, county)
);
'''
CREATE_CASES_INDEX = '''
CREATE INDEX IF NOT EXISTS cases_idx ON cases (day);
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

INSERT_DUMMY_SUBDIVISION = 'INSERT INTO subdivision (id, name, country) VALUES (0, null, 0) ON CONFLICT DO NOTHING;'

INSERT_COUNTY = '''
INSERT INTO county (name, subdivision) VALUES (%s, %s) ON CONFLICT DO NOTHING;
'''

INSERT_DUMMY_COUNTY = 'INSERT INTO county (id, name, subdivision) VALUES (0, null, 0) ON CONFLICT DO NOTHING;'

GET_COUNTRY = '''
SELECT * FROM country WHERE name = %s;
'''

GET_SUBDIVISION = '''
SELECT * FROM subdivision WHERE name = %s;
'''

GET_COUNTY = '''
SELECT * FROM county WHERE name = %s AND subdivision = %s;
'''

INSERT_CASES = '''
INSERT INTO cases (
    day, country, subdivision, county, positive_cases, deaths, recovered
) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
'''

INCREMENT_CASES = '''
INSERT INTO cases 
    (day, country, subdivision, county, positive_cases, deaths, recovered) VALUES (%s, %s, %s, %s, %s, %s, %s) 
    ON CONFLICT (day, country, subdivision, county) DO UPDATE SET 
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

# Johns Hopkins currently doesn't track data broken out by borough so delete it...
DELETE_NEW_YORK_BORO_DATA = '''
DELETE 
FROM   cases 
WHERE  cases.county IN 
    ( 
        SELECT     county.id 
        FROM       county 
        INNER JOIN subdivision 
        ON         county.subdivision
                    = subdivision.id 
        WHERE      county.NAME IN ('Queens', 
                                    'Bronx', 
                                    'Richmond', 
                                    'Kings') 
        AND        subdivision.NAME='New York');
'''

DELETE_BOROS = '''
DELETE FROM county 
WHERE  county.id IN (SELECT county.id 
                     FROM   county 
                            INNER JOIN subdivision 
                                    ON county.subdivision = subdivision.id 
                     WHERE  county.NAME IN ( 'Queens', 'Bronx', 'Richmond', 
                                             'Kings' ) 
                            AND subdivision.NAME = 'New York'); 
'''

COUNTRY_MOVING_AVERAGES_VIEW = '''
CREATE OR REPLACE VIEW view_moving_averages_country AS (SELECT day,
       Avg(positive_cases)
         over (
           PARTITION BY country
           ORDER BY day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) :: INTEGER AS
       positive_cases,
       Avg(deaths)
         over (
           PARTITION BY country
           ORDER BY day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) :: INTEGER AS
       deaths,
       Avg(recovered)
         over (
           PARTITION BY country
           ORDER BY day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) :: INTEGER AS
       recovered,
       country AS ref_id
FROM   (SELECT day,
               SUM(positive_cases) AS positive_cases,
               SUM(deaths)         AS deaths,
               SUM(recovered)      AS recovered,
               country AS country
        FROM   cases
        GROUP  BY country, day
        ORDER  BY country, day) AS cases); 
'''

SUBDIVISION_MOVING_AVERAGES_VIEW = '''
CREATE OR REPLACE VIEW view_moving_averages_subdivision AS (SELECT day,
       Avg(positive_cases)
         over (
           PARTITION BY subdivision
           ORDER BY day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) :: INTEGER AS
       positive_cases,
       Avg(deaths)
         over (
           PARTITION BY subdivision
           ORDER BY day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) :: INTEGER AS
       deaths,
       Avg(recovered)
         over (
           PARTITION BY subdivision
           ORDER BY day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) :: INTEGER AS
       recovered,
       subdivision AS ref_id
FROM   (SELECT day,
               SUM(positive_cases) AS positive_cases,
               SUM(deaths)         AS deaths,
               SUM(recovered)      AS recovered,
               subdivision AS subdivision
        FROM   cases
        GROUP  BY subdivision, day
        ORDER  BY subdivision, day) AS cases); 
'''

COUNTY_MOVING_AVERAGES_VIEW = '''
CREATE OR REPLACE VIEW view_moving_averages_county AS (SELECT day,
       Avg(positive_cases)
         over (
           PARTITION BY county
           ORDER BY day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) :: INTEGER AS
       positive_cases,
       Avg(deaths)
         over (
           PARTITION BY county
           ORDER BY day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) :: INTEGER AS
       deaths,
       Avg(recovered)
         over (
           PARTITION BY county
           ORDER BY day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) :: INTEGER AS
       recovered,
       county AS ref_id
FROM   (SELECT day,
               SUM(positive_cases) AS positive_cases,
               SUM(deaths)         AS deaths,
               SUM(recovered)      AS recovered,
               county AS county
        FROM   cases
        GROUP  BY county, day
        ORDER  BY county, day) AS cases); 
'''

COUNTRY_DERIVATIVE_VIEW = '''
CREATE OR REPLACE VIEW view_derivative_country AS (SELECT day,
       positive_cases - lag(positive_cases)
         over (
           PARTITION BY ref_id
           ORDER BY day) :: INTEGER AS
       positive_cases,
       deaths - lag(deaths)
         over (
           PARTITION BY ref_id
           ORDER BY day) :: INTEGER AS
       deaths,
       recovered - lag(recovered)
         over (
           PARTITION BY ref_id
           ORDER BY day) :: INTEGER AS
       recovered,
       ref_id AS ref_id
    FROM   (SELECT day,
                SUM(positive_cases) AS positive_cases,
                SUM(deaths)         AS deaths,
                SUM(recovered)      AS recovered,
                ref_id AS ref_id
            FROM   view_moving_averages_country
            GROUP  BY ref_id, day) AS cases);
'''

SUBDIVISION_DERIVATIVE_VIEW = '''
CREATE OR REPLACE VIEW view_derivative_subdivision AS (SELECT day,
       positive_cases - lag(positive_cases)
         over (
           PARTITION BY ref_id
           ORDER BY day) :: INTEGER AS
       positive_cases,
       deaths - lag(deaths)
         over (
           PARTITION BY ref_id
           ORDER BY day) :: INTEGER AS
       deaths,
       recovered - lag(recovered)
         over (
           PARTITION BY ref_id
           ORDER BY day) :: INTEGER AS
       recovered,
       ref_id AS ref_id
FROM   (SELECT day,
               SUM(positive_cases) AS positive_cases,
               SUM(deaths)         AS deaths,
               SUM(recovered)      AS recovered,
               ref_id AS ref_id
        FROM   view_moving_averages_subdivision
        GROUP  BY ref_id, day) AS cases);
'''

COUNTY_DERIVATIVE_VIEW = '''
CREATE OR REPLACE VIEW view_derivative_county AS (SELECT day,
       positive_cases - lag(positive_cases)
         over (
           PARTITION BY ref_id
           ORDER BY day) :: INTEGER AS
       positive_cases,
       deaths - lag(deaths)
         over (
           PARTITION BY ref_id
           ORDER BY day) :: INTEGER AS
       deaths,
       recovered - lag(recovered)
         over (
           PARTITION BY ref_id
           ORDER BY day) :: INTEGER AS
       recovered,
       ref_id AS ref_id
FROM   (SELECT day,
               SUM(positive_cases) AS positive_cases,
               SUM(deaths)         AS deaths,
               SUM(recovered)      AS recovered,
               ref_id AS ref_id
        FROM   view_moving_averages_county
        GROUP  BY ref_id, day) AS cases);
'''


conn = psycopg2.connect(database='covid')
cur = conn.cursor()

cur.execute(CREATE_COUNTRY_TABLE)
cur.execute(CREATE_SUBDIVISION_TABLE)
cur.execute(CREATE_SUBDIVISION_INDEX)
cur.execute(CREATE_COUNTY_TABLE)
cur.execute(CREATE_COUNTY_INDEX)
cur.execute(CREATE_COUNTRY_INDEX)
cur.execute(INSERT_DUMMY_COUNTRY)
cur.execute(INSERT_DUMMY_SUBDIVISION)
cur.execute(INSERT_DUMMY_COUNTY)
cur.execute(CREATE_CASES_TABLE)
cur.execute(CREATE_CASES_INDEX)
cur.execute(TRACKED_DATES_TABLE)
cur.execute(COUNTRY_MOVING_AVERAGES_VIEW)
cur.execute(SUBDIVISION_MOVING_AVERAGES_VIEW)
cur.execute(COUNTY_MOVING_AVERAGES_VIEW)
cur.execute(COUNTRY_DERIVATIVE_VIEW)
cur.execute(SUBDIVISION_DERIVATIVE_VIEW)
cur.execute(COUNTY_DERIVATIVE_VIEW)
conn.commit()


inconsistent_recovered_subdivision_data_countries = set()


# on 3/18 there were a bunch of NaN values
def nan_to_int(val):
    if val is np.nan:
        return 0
    else:
        return int(val)


def insert_ts_row(row, us=False, deaths=deaths, recovered=recovered):
    if us:
        province_state = 'Province_State'
        country_region = 'Country_Region'
        county_col_head = 'Admin2'
    else:
        province_state = 'Province/State'
        country_region = 'Country/Region'
        county_col_head = None
    
    subdivision = row[province_state]
    country = row[country_region]
    if us:
        county = row[county_col_head]
    else:
        if country == 'US':
            return
        county = None

    if not isinstance(subdivision, float):
        deaths_df = deaths.loc[(deaths[province_state] == subdivision) & (deaths[country_region] == country)]
        if isinstance(recovered, DataFrame):
            recovered_df = recovered.loc[(recovered[province_state] == subdivision) & (recovered[country_region] == country)]
        cur.execute(INSERT_COUNTRY, (country,))
        subdivision = subdivision.strip()

        conn.commit()
        cur.execute(GET_COUNTRY, [country])
        country_id, _ = cur.fetchone()
        conn.commit()
        cur.execute(INSERT_SUBDIVISION, [subdivision, country_id])
        cur.execute(GET_SUBDIVISION, [subdivision])
        subdivision_id, _, _ = cur.fetchone()
        if us and not isinstance(county, float):
            cur.execute(INSERT_COUNTY, [county, subdivision_id])
            conn.commit()
            cur.execute(GET_COUNTY, [county, subdivision_id])
            county_id, _, _ = cur.fetchone()
            deaths_df = deaths_df.loc[deaths_df[county_col_head] == county]
        else:
            county_id = 0

    else:
        deaths_df = deaths.loc[(deaths[province_state].isnull()) & (deaths[country_region] == country)]
        if isinstance(recovered, DataFrame):
            recovered_df = recovered.loc[(recovered[province_state].isnull()) & (recovered[country_region] == country)]
        cur.execute(INSERT_COUNTRY, (country,))
        conn.commit()
        cur.execute(GET_COUNTRY, [country])
        country_id, _ = cur.fetchone()

        subdivision_id = 0  # dummy subdivision for unique constraint
        county_id = 0

    for date in dates:
        exclude_recoveries = False
        cur.execute('SELECT EXISTS (SELECT 1 FROM tracked_dates WHERE day=%s)', [date])
        is_date_tracked = cur.fetchone()[0]
        if is_date_tracked:
            continue
        cases_date = row[date]
        deaths_date = deaths_df.iloc[0][date]
        if isinstance(recovered, DataFrame):
            try:
                # some countries don't have subdivision recoveries data -- we ignore these countries for now
                recovered_date = recovered_df.iloc[0][date]
            except:            
                exclude_recoveries = True
        else:
            exclude_recoveries = True
        if cases_date is np.nan:
            continue  # no data for this day (yet?) -- allowing other numbers to be nan, but not this one
        cur.execute(
            INSERT_CASES, (
                date, 
                country_id, 
                subdivision_id,
                county_id,
                nan_to_int(cases_date), 
                nan_to_int(deaths_date), 
                nan_to_int(recovered_date) if not exclude_recoveries else 0
            )
        )
        conn.commit()


for _, row in cases.iterrows():
    insert_ts_row(row)

cases = pd.read_csv('../COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv')
deaths = pd.read_csv('../COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv')
for _, row in cases.iterrows():
    dates = cases.columns[11:]
    insert_ts_row(row, us=True, deaths=deaths, recovered=None)


for date in dates:
    cur.execute(INSERT_TRACKED_DATE, [date])
    conn.commit()

cur.execute(DELETE_NEW_YORK_BORO_DATA)
cur.execute(DELETE_BOROS)
conn.commit()