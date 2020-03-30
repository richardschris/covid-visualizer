import math
import os
import pandas as pd
import dateutil.parser as dparser

class ReportBuilder:
    """ Parses the raw CSV files and turnt it into a searchable structure """

    def _try_get_value(self, csv, value):
        try:
            val = csv[value]
            return val
        except KeyError:
            return None

    def _get_daily_reports(self, us_country_data_dir):
        """ Returns the daily report data that contains the province, subdivision and country counts per days"""
        daily_reports = os.listdir(us_country_data_dir)
        reports = []
        for daily_report in daily_reports:
            if daily_report.lower().endswith('.csv'):
                daily_report_csv = pd.read_csv(os.path.join(us_country_data_dir, daily_report))

                for _, row in daily_report_csv.iterrows():
                    province = self._try_get_value(row, 'Province_State')
                    if not province:
                        province = self._try_get_value(row, 'Province/State')

                    country_code = self._try_get_value(row, 'Country_Region')
                    if not country_code:
                        country_code = self._try_get_value(row, 'Country/Region')

                    reports.append(DailyReport(
                        date = daily_report.lower()[:-4],
                        subdivision = self._try_get_value(row, 'Admin2'), 
                        province = province, 
                        country_code = country_code, 
                        confirmed = self._try_get_value(row, 'Confirmed'), 
                        deaths= self._try_get_value(row, 'Deaths'), 
                        recovered = self._try_get_value(row, 'Recovered'), 
                        active = self._try_get_value(row, 'Active')))
        return reports

    def parse(self, base_path):
        for report in self._get_daily_reports(base_path):
            if report.country_code not in self.country_codes_to_reports_by_date:
                self.country_codes_to_reports_by_date[report.country_code] = {report.get_date_string(): [report]}
            else:
                if report.get_date_string() not in self.country_codes_to_reports_by_date[report.country_code]:
                    self.country_codes_to_reports_by_date[report.country_code][report.get_date_string()] = [report]
                else:
                    self.country_codes_to_reports_by_date[report.country_code][report.get_date_string()].append(report)
        return self.country_codes_to_reports_by_date

    def __init__(self):
        # Country_Code -> Dates -> Reports
        self.country_codes_to_reports_by_date = {}

class DailyReport:

    def __init__(self, date = '', subdivision = '', province  = '', country_code = '', confirmed = 0, deaths = 0, recovered = 0, active = 0):
        self.date = dparser.parse(date)
        self.subdivision = subdivision if isinstance(subdivision, str) else ''
        self.province = province  if isinstance(province, str) else ''
        self.country_code = country_code if isinstance(country_code, str) else ''
        self.confirmed = confirmed
        self.deaths = deaths
        self.recovered = recovered
        self.active = active

    def get_date_string(self):
        """ Return the date string in the same format as the time series reports """
        return self.date.strftime("%m/%d/%y").lstrip("0").replace(" 0", " ")

    def __str__(self):
        if not self.subdivision and not self.province:
            return f'[{self.date}] - {self.country_code}. Confirmed: {self.confirmed}. Deaths: {self.deaths}, Recovered: {self.recovered}, Active: {self.active}'
        if not self.subdivision:
            return f'[{self.date}] - {self.province}, {self.country_code}. Confirmed: {self.confirmed}. Deaths: {self.deaths}, Recovered: {self.recovered}, Active: {self.active}'
        if not self.province:
            return f'[{self.date}] - {self.subdivision}, {self.country_code}. Confirmed: {self.confirmed}. Deaths: {self.deaths}, Recovered: {self.recovered}, Active: {self.active}'
        return f'[{self.date}] - {self.province}, {self.country_code}. Confirmed: {self.confirmed}. Deaths: {self.deaths}, Recovered: {self.recovered}, Active: {self.active}'