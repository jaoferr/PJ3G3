import requests
import bs4
import json
import sys
from collections import defaultdict
import csv
import pickle
import pandas as pd
import argparse
import time

class CustomTimer:

    def __init__(self):
        pass

    def start_timer(self):
        self.start = time.time()

    def end_timer(self):
        end = time.time()
        time_elapsed = end - self.start
        time_elapsed_formated = round(time_elapsed, 3)
        print(f'\n\nTime elapsed: {time_elapsed_formated}s')

class NFLSS:

    def __init__(self, start_year, end_year, export_data, export_stat, export_schedule):
        self.base_url = r'https://www.pro-football-reference.com'
        self.season_url = self.base_url + r'/years/{}/'
        # self.team_schedule_url = self.base_url + r'/years/{}/games.htm'
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
        # export switches
        self.export_data = export_data
        self.export_stat = export_stat
        self.export_schedule = export_schedule

    def build_season_url(self):
        self.current_url = self.season_url.format(self.current_year)

    def make_soup(self):
        ''' Requests current url and generates new soup '''
        r = requests.get(self.current_url)
        self.soup = bs4.BeautifulSoup(r.text, 'html.parser')      
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

            data[team_name] = current_team

        self.current_table_data = data
        return data

    def extract_stat_descriptions(self):
        stat_headers = self.current_table.find_all('th', {'class': 'poptip'})
        descriptions = {}
        for header in stat_headers:
            stat_name = header.attrs['data-stat']
            label = header.attrs['aria-label']
            
            descriptions[stat_name] = label

        return descriptions

    def build_team_schedule_url(self, link):
        self.current_team_schedule_url = self.base_url + link

    def make_team_schedule_soup(self):
        ''' Requests current season schedule url and generates a soup '''
        r = requests.get(self.current_team_schedule_url)
        self.current_team_schedule_soup = bs4.BeautifulSoup(r.text, 'html.parser')

    def extract_season_schedule(self, team_name):
        table = self.current_team_schedule_soup.find('table', {'id': 'games'})
        tbody = table.find('tbody')
        team_schedule = {}
        # team_name = self.current_team_schedule_soup.find('h1', {'itemprop': 'name'}).find_all('span')[1].text
        # games in season
        for irow, row in enumerate(tbody.find_all('tr', {'class': ''})):
            row_stats = {}
            # game stats
            for col in row.find_all('td'):  # each column in current row
                stat = col['data-stat']  # stat name
                stat_value = col.text
                row_stats[stat] = stat_value
            
            team_schedule[irow] = row_stats

        return {team_name: team_schedule}

    def setup(self, year):
        ''' Setup season page html soup for a given year '''
        self.current_year = year
        self.build_season_url()
        self.make_soup()

    def run_multiple_years(self):
        ''' Runs from start_year to end_year and outputs to json file'''
        # if args are correct
        print(f'Running from {self.start_year} to {self.end_year}')
        all_data = {}
        all_stat_descriptions = {}
        all_team_schedules = {}

        for year in range(self.end_year, self.start_year - 1, -1):
            print(year)
            self.setup(year)
            year_data = defaultdict(dict)
            stat_descriptions = {}
            team_schedules = {}
            tables_to_extract = [
                'all_AFC', 'all_NFC', 'all_team_stats', 'all_passing',
                'all_rushing', 'all_returns', 'all_kicking',
                'all_team_scoring', 'all_team_conversions', 'all_drives'
            ]

            for table in tables_to_extract:
                print(f'\t {table:<30}', end='', flush=False)
                try:
                    self.get_tables(table_id=table)  # get table soup
                    data = self.extract_season_data()  # extract data from soup
                    descriptions = self.extract_stat_descriptions()  # extract stat descriptions from current table
                    stat_descriptions = {**stat_descriptions, **descriptions}  # bundle everything into a dict
                    print('OK')

                    if table in ['all_AFC', 'all_NFC']:  
                        team_page_links = self.current_table.find_all('a')  # individual team page
                        for link in team_page_links:
                            team_name = link.text
                            print(f'\t\tExtracting schedules: {team_name:<40}', end='', flush=False)
                            self.build_team_schedule_url(link.attrs['href'])
                            self.make_team_schedule_soup()  # individual team page soup
                            current_team_schedule = self.extract_season_schedule(team_name=team_name)  # extract team schedule, week by week
                            team_schedules = {**team_schedules, **current_team_schedule}  # bundle everything into a dict
                            print('OK')
                    
                except TypeError:
                    print('X')
                    data = {}

                for team in data.keys():
                    for stat_name, stat_value in data[team].items():
                        year_data[team][stat_name] = stat_value

            all_data[self.current_year] = year_data
            all_stat_descriptions = {**all_stat_descriptions, **stat_descriptions}
            all_team_schedules[self.current_year] = team_schedules

            self.data = all_data
            self.stat_descriptions = all_stat_descriptions
            self.team_schedules = all_team_schedules

    def dump_stat_descriptions(self):
        ''' Dumps stat dictionary to a JSON file '''
        local_filename = self.export_filename + '_stat_descriptions.json'
        with open(local_filename, 'w') as j:
            json.dump(self.stat_descriptions, j)
        print(f'Stat descriptions saved to {local_filename}')

    def dump_team_schedules(self):
        ''' Dumps all teams schedules to a CSV file '''
        local_filename = self.export_filename + '_team_schedule.csv'
        print(f'Team schedules saved to {local_filename}')

        reoriented_data = {
            (i, j, k): self.team_schedules[i][j][k]
                    for i in self.team_schedules.keys()
                    for j in self.team_schedules[i].keys()
                    for k in self.team_schedules[i][j].keys()
        }
        df = pd.DataFrame.from_dict(reoriented_data, orient='index')
        df.to_csv(local_filename, sep=';', encoding='utf-8')

    def dump_to_csv(self):
        ''' Dumps season data do CSV file '''
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

    def export(self):
        if self.export_data:
            if self.export_data not in self.export_methods:
                print('Using default export method (csv)')
                self.export_methods['csv']()
            else:
                self.export_methods[self.export_data]()
            
            print('Done exporting to', self.export_data)
        
        if self.export_schedule:
            self.dump_team_schedules()
        
        if self.export_stat:
            self.dump_stat_descriptions()

if __name__ == '__main__':
    timer = CustomTimer()
    timer.start_timer()
    # CLI args
    parser = argparse.ArgumentParser(description='CLI Testing')

    # Required
    parser.add_argument('start_year', type=int, help='Initial year')
    parser.add_argument('end_year', type=int, help='End year')

    # Optional
    parser.add_argument('-o', type=str, help='Format to output data')
    parser.add_argument('-stat', action='store_true', help='Export stat descriptions')
    parser.add_argument('-ts', action='store_true', help='Export team schedules')

    args = vars(parser.parse_args())

    try:
        nfl = NFLSS(
            start_year=args['start_year'],
            end_year=args['end_year'],
            export_data=args['o'],
            export_stat=args['stat'],
            export_schedule=args['ts']
        )
        nfl.run_multiple_years()
        nfl.export()
    except Exception as e:
        print(e)

    timer.end_timer()
