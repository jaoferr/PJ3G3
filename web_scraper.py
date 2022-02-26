'''
Asyncio + multiprocessing.Pool
'''
import asyncio
import aiohttp
import argparse
import time
import bs4
import pandas as pd
import os
import multiprocessing
import pickle
from collections import defaultdict
from tqdm import tqdm


class CustomTimer:

    def __init__(self):
        self.start = time.time()

    def start_timer(self):
        self.start = time.time()

    def end_timer(self):
        end = time.time()
        time_elapsed = end - self.start
        time_elapsed_formated = round(time_elapsed, 3)
        print(f'\n\nTime elapsed: {time_elapsed_formated}s')

    def end_timer_no_print(self):
        end = time.time()
        time_elapsed = end - self.start
        time_elapsed_formated = round(time_elapsed, 3)
        return time_elapsed_formated

class AsyncNFLSS:

    def __init__(
        self, start_year:int, end_year:int,
        export_data: bool, export_stat: bool,
        export_schedule: bool, export_pickle: bool,
        max_workers: int=None
    ):
        self.base_url = r'https://www.pro-football-reference.com'
        self.season_url = self.base_url + r'/years/{}/'
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
        self.export_filename = os.path.join('data', f'{self.start_year}-{self.end_year}')
        # export switches
        self.export_data = export_data
        self.export_stat = export_stat
        self.export_schedule = export_schedule
        self.export_pickle = export_pickle

        if os.name == 'nt':  # windows
            self.encoding = 'ANSI'
        else:
            # self.encoding = 'utf-8'
            self.encoding = 'latin-1'

        self.max_workers = max_workers or multiprocessing.cpu_count()

    def setup(self):
        self.season_data = defaultdict(dict)
        self.season_html = {}
        
        self.stat_descriptions = {}
        
        self.team_schedules = defaultdict(dict)
        self.team_html = {}
        self.team_links = defaultdict(dict)

        self.tables_to_extract = [
            'all_AFC', 'all_NFC', 'all_team_stats', 'all_passing',
            'all_rushing', 'all_returns', 'all_kicking',
            'all_team_scoring', 'all_team_conversions', 'all_drives'
        ]

    async def fetch_season_page(self, session, year):
        print(f'\tFetching {year} season')
        url = self.season_url.format(year)
        r = await session.request(method='GET', url=url)
        r.raise_for_status()  # not sure what this does
        html = await r.text(encoding=self.encoding)
        self.season_html[year] = html
        # return html

    async def fetch_all_seasons(self):
        print('Fetching season pages')
        tasks = []
        connector = aiohttp.TCPConnector(limit=10)  # avoid spamming the target
        async with aiohttp.ClientSession(connector=connector) as session:
            for year in range(self.end_year, self.start_year - 1, -1):
                tasks.append(
                    self.fetch_season_page(session, year)
                )
            # print('Done')
            return await asyncio.gather(*tasks)

    async def fetch_team_page(self, session, url, team_name, year):
        print(f'\tFetching {url}')
        r = await session.request(method='GET', url=url)
        r.raise_for_status()
        html = await r.text(encoding=self.encoding)
        self.team_html[year][team_name] = html
        print(f'\tDone fetching {url}')

    async def fetch_all_team_pages(self):
        print(f'Fetching team pages')
        tasks = []
        connector = aiohttp.TCPConnector(limit=10)  # avoid spamming the target
        async with aiohttp.ClientSession(connector=connector) as session:
            for year in self.team_links.keys():
                self.team_html[year] = {}
                print(year)
                for team_name, team_url in self.team_links[year].items():
                    tasks.append(
                        self.fetch_team_page(session, team_url, team_name, year)
                    )
            return await asyncio.gather(*tasks)

    def get_team_page_links(self, html, year):
        soup = bs4.BeautifulSoup(html, 'html.parser')
        team_page_links = soup.find_all('a')  # individual team page
        links = {}
        for link in team_page_links:
            team_name = link.text
            url = self.base_url + link.attrs['href']
            links[team_name] = url
        return links

    def uncomment_table(self, html):
        # uncomment all html code, needed for some tables
        for comment in html(text=lambda text: isinstance(text, bs4.Comment)):
            tag = bs4.BeautifulSoup(comment, 'html.parser')
            comment.replace_with(tag)

        return html
        
    def extract_data_from_table(self, table_html):
        tbody = table_html.find('tbody')
        season_data = {}
        for row in tbody.find_all('tr', {'class': ''}):
            team_name = row.find(attrs={'data-stat': 'team'}).text
            team_name = team_name.replace('*', '').replace('+', '')
            current_team = {}

            # season stats
            for col in row.find_all('td'):
                stat_name = col['data-stat']
                stat_value = col.text
                current_team[stat_name] = stat_value

            season_data[team_name] = current_team
        return season_data

    def process_season_soup(self, args):
        year, html = args
        print(f'\tProcessing {year} season')
        soup = bs4.BeautifulSoup(html, 'html.parser')
        links_html = ''
        season_data = defaultdict(defaultdict)
        for table_id in self.tables_to_extract:
            table_html = soup.find('div', {'class': 'table_wrapper',
                                           'id': table_id})
            if table_html is None:
                continue
            table_html = self.uncomment_table(table_html)

            table_data = self.extract_data_from_table(table_html)
            if table_id in ['all_AFC', 'all_NFC']:
                links_html += str(table_html)

            for team_name, team_stats in table_data.items():
                season_data[team_name] = season_data[team_name] | team_stats
        links = self.get_team_page_links(links_html, year)

        print(f'Done processing {year} season')
        return {year: {'season_data': season_data, 'team_links': links}}

    def process_all_seasons(self):
        print('Processing season pages')
        timer = CustomTimer()
        tasks = []
        for year, html in self.season_html.items():
            tasks.append((year, html))

        with multiprocessing.Pool(self.max_workers) as pool:
            results = pool.map(self.process_season_soup, tasks)

        for res in results:
            for year in res.keys():
                self.season_data[year] = res[year]['season_data']
                self.team_links[year] = res[year]['team_links']
        print(f'Done processing season pages in {timer.end_timer_no_print()}s')

    def process_team_page(self, args):
        html, team_name, year = args
        soup = bs4.BeautifulSoup(html, 'html.parser')
        table = soup.find('table', {'id': 'games'})
        tbody = table.find('tbody')
        team_schedule = {}
        # games in season
        for irow, row in enumerate(tbody.find_all('tr', {'class': ''})):
            row_stats = {}
            # game stats
            for col in row.find_all('td'):  # each column in current row
                stat = col['data-stat']  # stat name
                stat_value = col.text
                row_stats[stat] = stat_value

            row_stats['week_num'] = row.find('th', {'data-stat': 'week_num'}).text
            team_schedule[irow] = row_stats
            
        print(f'Done processing {team_name} {year} team page.')
        return {year: {team_name: team_schedule}}

    def process_all_team_pages(self):
        timer = CustomTimer()
        print('Processing team pages')
        tasks = []
        for year in self.team_html.keys():
            self.team_schedules[year] = {}
            for team_name, team_html in self.team_html[year].items():
                tasks.append((team_html, team_name, year))

        with multiprocessing.Pool(self.max_workers) as pool:
            results = pool.map(self.process_team_page, tasks)

        for res in results:
            for year in res.keys():
                for team in res[year].keys():
                    self.team_schedules[year][team] = res[year][team]

        print(f'Done processing team pages in {timer.end_timer_no_print()}s')

    def run_fetch_all_seasons(self):
        timer = CustomTimer()
        asyncio.run(self.fetch_all_seasons())
        print(f'Done fetching season pages in {timer.end_timer_no_print()}s')

    def run_fetch_all_team_pages(self):
        timer = CustomTimer()
        asyncio.run(self.fetch_all_team_pages())
        print(f'Done fetching team pages in {timer.end_timer_no_print()}s')

    def run(self):
        self.setup()
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        self.run_fetch_all_seasons()
        self.process_all_seasons()
        self.run_fetch_all_team_pages()
        self.process_all_team_pages()

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
        filename = self.export_filename + '.csv'
        reoriented_data = {
            (i, j): self.season_data[i][j]
                    for i in self.season_data.keys()
                    for j in self.season_data[i].keys()
        }
        df = pd.DataFrame.from_dict(reoriented_data, orient='index')
        df.to_csv(filename, sep=';', encoding='utf-8')
        print('Exported season data to', filename)

    def dump_to_pickle(self):
        local_filename = self.export_filename + '.pickle'
        with open(local_filename, 'wb') as file:
            pickle.dump((self.team_schedules, self.season_data), file)

    def export(self):
        if not os.path.exists(os.path.join('.', 'data')):
            os.makedirs(os.path.join('.', 'data'))

        if self.export_data:
            self.dump_to_csv()

        if self.export_schedule:
            self.dump_team_schedules()
        
        if self.export_pickle:
            self.dump_to_pickle()

        # if self.export_stat:
            # self.dump_stat_descriptions()


if __name__ == '__main__':
    timer = CustomTimer()
    timer.start_timer()
    # CLI args
    parser = argparse.ArgumentParser(description='CLI Testing')

    # Required
    parser.add_argument('start_year', type=int, help='Initial year')
    parser.add_argument('end_year', type=int, help='End year')

    # Optional
    parser.add_argument('-o', action='store_true', help='Export data')
    parser.add_argument('-stat', action='store_true', help='Export stat descriptions')
    parser.add_argument('-ts', action='store_true', help='Export team schedules')
    parser.add_argument('-pickle', action='store_true', help='Export data as .pickle')
    parser.add_argument('-w', type=int, help='How many workers to use (default = cpu_count)')
    args = vars(parser.parse_args())
    
    try:
        nfl = AsyncNFLSS(
            start_year=args['start_year'],
            end_year=args['end_year'],
            export_data=args['o'],
            export_stat=args['stat'],
            export_schedule=args['ts'],
            export_pickle=args['pickle'],
            max_workers=args['w']
        )
        nfl.run()
        nfl.export()
        timer.end_timer()

    except Exception as e:
        print(e)
        input()

