import requests
import bs4
import json


class NFLSS:
    def __init__(self):
        self.base_url = r'https://www.pro-football-reference.com'
        self.season_url = self.base_url + r'/years/{}/'

        self.current_url = ''  # placeholder

    def build_season_url(self, year):
        self.current_url = self.season_url.format(year)

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
        data = []
        tbody = self.current_table.find('tbody')

        for row in tbody.find_all('tr', {'class': ''}):
            team_name = row.find('th').text
            stat_page_url = row.find('th').find('a')['href']

            current_team = {
                'team_name': team_name,
                'stat_page_url': stat_page_url
            }
            print('\t', team_name)
            # season stats
            for col in row.find_all('td'):  # each column in current row
                stat = col['data-stat']  # stat name
                stat_value = col.text
                current_team[stat] = stat_value

            # team season stats
            self.build_team_url(stat_page_url)
            self.make_team_soup()
            team_stats = self.extract_team_data()

            current_team = {**current_team, **team_stats}

            data.append(current_team)

        self.current_table_data = data
        return data

    def setup(self, year):
        ''' Setup season page html soup for a given year '''
        self.build_season_url(year)
        self.make_soup()

    def run_multiple_years(self, start_year, end_year):
        ''' Runs from start_year to end_year and outputs to json file'''
        print(f'Running from {start_year} to {end_year}')
        for year in range(start_year, end_year + 1):
            print(year)
            self.setup(year)
            year_data = []
            for table in ['all_AFC', 'all_NFC']:
                print('\t', table)
                self.get_tables(table_id=table)
                data = self.extract_season_data()
                year_data += data

            dump_to_json(year_data, str(year))


def dump_to_json(data, filename='dump'):
    with open(filename + '.json', 'w') as j:
        json.dump(data, j)


if __name__ == '__main__':
    nfl = NFLSS()
    nfl.run_multiple_years(1970, 2020)
