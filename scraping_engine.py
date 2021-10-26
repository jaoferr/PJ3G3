import requests
import bs4
import json
import sys
from collections import defaultdict
import csv
import pickle
import pandas as pd


class NFLSS:

    def __init__(self, start_year=2010, end_year=2020):
        self.base_url = r'https://www.pro-football-reference.com'
        self.season_url = self.base_url + r'/years/{}/'
        self.current_url = ''  # placeholder

        # check year args
        try:
            start_year = int(start_year)
            end_year = int(end_year)
        except ValueError:
            raise TypeError(f'Invalid arguments: {start_year} or {end_year} are not numbers.')

        if not start_year or not end_year:
            print('No arguments provided, using default values.')
        elif ((start_year < 1970) or (end_year > 2020)):
            raise ValueError(f'Please input years between 1970 and 2020.')

        self.start_year = start_year
        self.end_year = end_year

        # export stuff
        self.export_filename = filename = f'{self.start_year}-{self.end_year}'
        self.export_methods = {
            'json': self.dump_to_json,
            'csv': self.dump_to_csv,
            'pickle': self.dump_to_pickle
        }

    def build_season_url(self, year):
        self.current_url = self.season_url.format(year)
        self.current_year = year

    def build_team_url(self, team_url):
        self.current_team_url = self.base_url + team_url

    def make_soup(self):
        ''' Requests current url and generates new soup '''
        r = requests.get(self.current_url)
        self.soup = bs4.BeautifulSoup(r.text, 'html.parser')
        return self.soup

    def make_team_soup(self):
        ''' Requests current team url and generates new team soup '''
        r = requests.get(self.current_team_url)
        self.team_soup = bs4.BeautifulSoup(r.text, 'html.parser')
        return self.team_soup

    def get_tables(self, table_id):
        ''' Retrieves given table_id's html '''
        current_table = self.soup.find('div', {'class': 'table_wrapper',
                                               'id': table_id})

        # uncomment everything, solves issues with some tables
        for comment in current_table(text=lambda text: isinstance(text, bs4.Comment)):
            tag = bs4.BeautifulSoup(comment, 'html.parser')
            comment.replace_with(tag)
        
        self.current_table = current_table
        return current_table

    def get_team_tables(self, table_id):
        ''' Retrieves given table_id's html '''
        current_table = self.team_soup.find('div', {'class': 'table_wrapper',
                                                    'id': table_id})
        self.current_table = current_table
        return current_table

    def extract_team_data(self):
        ''' Extracts data from team stat page '''
        table_id = 'all_team_stats'
        self.get_team_tables(table_id)
        tbody = self.current_table.find('tbody')

        data = {}
        for col in tbody.find('tr').find_all('td'):
            stat = col['data-stat']  # stat name
            stat_value = col.text
            data[stat] = stat_value

        return data

    def extract_season_data(self):
        ''' Extracts data from season page '''
        data = {}
        tbody = self.current_table.find('tbody')

        for row in tbody.find_all('tr', {'class': ''}):
            team_name = row.find(attrs={'data-stat': 'team'}).text
            team_name = team_name.replace('*', '').replace('+', '')
            current_team = {}

            # season stats
            for col in row.find_all('td'):  # each column in current row
                stat = col['data-stat']  # stat name
                stat_value = col.text
                current_team[stat] = stat_value

            # team season stats
            # self.build_team_url(stat_page_url)
            # self.make_team_soup()
            # team_stats = self.extract_team_data()
            # current_team = {**current_team}
            data[team_name] = current_team
            

        self.current_table_data = data
        return data

    def setup(self, year):
        ''' Setup season page html soup for a given year '''
        self.build_season_url(year)
        self.make_soup()

    def run_multiple_years(self):
        ''' Runs from start_year to end_year and outputs to json file'''
        # if args are correct
        print(f'Running from {self.start_year} to {self.end_year}')
        all_data = {}
        for year in range(self.end_year, self.start_year - 1, -1):
            print(year)
            self.setup(year)
            year_data = defaultdict(dict)
            tables_to_extract = [
                'all_AFC', 'all_NFC', 'all_team_stats', 'all_passing',
                'all_rushing', 'all_returns', 'all_kicking',
                'all_team_scoring', 'all_team_conversions', 'all_drives'
            ]

            for table in tables_to_extract:
                print(f'\t {table:<30}', end='', flush=False)
                try:
                    self.get_tables(table_id=table)
                    data = self.extract_season_data()
                    print('OK')
                except TypeError:
                    print('X')
                    data = {}

                for team in data.keys():
                    # print('\t', team)
                    for stat_name, stat_value in data[team].items():
                        # print('\t\t', stat_name)
                        year_data[team][stat_name] = stat_value

            all_data[self.current_year] = year_data
            self.data = all_data

    def dump_to_csv(self):
        reoriented_data = {
            (i, j): self.data[i][j]
                    for i in self.data.keys()
                    for j in self.data[i].keys()
        }
        df = pd.DataFrame.from_dict(reoriented_data, orient='index')
        df.to_csv(self.export_filename + '.csv', sep=';', encoding='utf-8')

    def dump_to_pickle(self):
        with open(self.export_filename + '.pickle', "wb") as output_file:
            pickle.dump(self.data, output_file)

    def dump_to_json(self):
        with open(self.export_filename + '.json', 'w') as j:
            json.dump(self.data, j)

    def export(self, method):
        if method not in self.export_methods:
            print('Using default export method (csv)')
            self.export_methods['csv']()
        else:
            self.export_methods[method]()
        
        print('Done exporting to', method)

if __name__ == '__main__':
    if len(sys.argv) > 1:
        try:
            nfl = NFLSS(sys.argv[1], sys.argv[2])
            nfl.run_multiple_years()
            nfl.export(sys.argv[3])
        except Exception as e:
            print(e)
