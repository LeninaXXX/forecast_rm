import argparse
import logging
import os
import decimal
import itertools as it

from datetime import datetime, date
from dateutil.relativedelta import relativedelta

import pyodbc

# tables
dly_tbl_pfx = 'GA_DIARIO'
pm_tbl_pfx = 'GA_MENSUALPARCIAL'
mon_tbl_pfx = 'GA_MENSUAL'

tbls_sfxs = ('', '_sinPlayer', '_xCanal', '_xDispositivo', '_xFuente', '_xHostname',
             '_xPagina', '_xPais', '_xPais_xDispositivo', '_xPlayer', '_xSocialMedia')
tbls_pfxs = (dly_tbl_pfx, pm_tbl_pfx, mon_tbl_pfx)

all_tbls_names = tuple(pfx + sfx for pfx, sfx in it.product(tbls_pfxs, tbls_sfxs))
db_tbls_names = tbls_pfxs

forecast_tbl = 'GA_FORECAST_NEW'

# logging config
os.makedirs('log/', exist_ok=True)
logging.basicConfig(
    filename=f'log/{datetime.now().strftime("%Y%m%dT%H%M%S")}.log',
    format='[%(levelname)s] %(funcName)s@%(asctime)s :: %(message)s',
    datefmt='%H:%M:%S',
    level=logging.INFO
)

class Table:
    def name(self):
        return self.table_name
    
    def __getitem__(self, i):
        return self.table[i]
    
    def __len__(self):
        return len(self.table)
    
    def __str__(self):
        return f'Table(table_name = \'{self.table_name}\', len = {self.__len__()})'

    def __iter__(self):
        return iter(self.table)

class DBTableContainer(Table):
    def __init__(self, table_name, table):
        self.table_name = table_name
        
        i = iter(table) if table else None
        rt0 = next(i) if i else None
        
        self._rt_desc_dict = {
            col_name : {                    
                'type' : rem[0],            
                'internal_size' : rem[2],   
                'precision' : rem[3],       
                'scale' : rem[4]            
            } for (col_name, *rem) in rt0.cursor_description
        } if rt0 else None

        self.table = DBTableContainer._curate_table(
            tuple(self._rt_desc_dict), 
            it.chain((rt0, ), i)
        ) if rt0 else []
    
    @staticmethod
    def _curate_table(cols_names, r_tbl):
        curated_tbl = []

        for row in r_tbl:
            row_dict = {}
            for i, field in enumerate(row):
                row_dict[cols_names[i]] = field
            curated_tbl.append(row_dict)    

        return curated_tbl
    
    def column_types(self):
        return {col_name : d['type'] for col_name, d in self._rt_desc_dict.items()}
    
    def column_names(self):
        return tuple(self._rt_desc_dict.keys())
 
class RunningTable(Table):
    def __init__(self, table_name, table, conv_tbl):
        """
        table: has to offer iterable rows, each one of which is a dict with keys being column names
        conv_tbl: a dict, each key a column name, its value a tuple of type constructors
        """
        self.name = table_name
        self.table = []

        for row in table:
            self.table.append({col : (conv_tbl[col](row[col]) if col in conv_tbl else row[col]) for col in row})

class SortedTable(RunningTable):
    def __init__(self, table_name, table, conv_tbl, sort_func=None):
        super().__init__(table_name, table, conv_tbl)
        
        SortedTable._sort(self.table, key=sort_func)
    
    @staticmethod
    def _sort(table, key=None):
        table.sort(key=key)

    def check_consistency(self):
        return len({row['FechaFiltro'] for row in self}) == len(self)

class Predictor:
    def __init__(self, tables):
        self.tables = tables
    
    def __getattr__(self, __name):
        return self.tables[__name]
    
    def predict(self, day=date.today()):
        return self._predict_w_1y(day)
    
    def predict_w(self, day=date.today(), weight=1.0):
        return self._predict_w_2y_weighted_average(day, weight)

class RatioPredictor(Predictor):
    def _historic(self, day):
        month = day.replace(day=1) + relativedelta(months=+1, days=-1)

        partial = [row for row in self.GA_MENSUALPARCIAL if row['FechaFiltro'] == day]
        monthly = [row for row in self.GA_MENSUAL if row['FechaFiltro'] == month]
        
        if len(partial) == 1 and len(monthly) == 1:
            return (partial[0], monthly[0])
        else:
            logging.error(f'Something went wrong when retrieving historic values')
            logging.error(f'\tday   = {day}')
            logging.error(f'\tmonth = {month}')
            logging.error(f'\t')
            logging.error(f'\tlen(partial) == {len(partial)}')
            logging.error(f'\tpartial == {partial}')
            logging.error(f'')
            logging.error(f'\tlen(monthly) == {len(monthly)}')
            logging.error(f'\tmonthly == {monthly}')
            logging.error(f'\t')
            return (None, None)

    def _ratio(self, day):
        # partial, monthly = self._historic(day - relativedelta(years=1))
        partial, monthly = self._historic(day) # - relativedelta(years=1))
        
        if partial and monthly:
            return partial['Users']/monthly['Users']
        else:
            logging.error(f'Unable to calculate partial_montly to monthly ratio')
            logging.error(f'\tday == {day}')
            logging.error(f'\t')
            return None

    def _predict_w_1y(self, day):
        return self._predict_w_2y_weighted_average(day, 1.0)
    
    def _predict_w_2y_weighted_average(self, day, weight):
        r1ya = self._ratio(day - relativedelta(years=1))
        r2ya = self._ratio(day - relativedelta(years=2))

        if r1ya and r2ya: 
            w_avg = weight * r1ya + (1 - weight) * r2ya 
        else:
            if r1ya:
                w_avg = r1ya
            else:
                logging.error(f'In calculating "weighted_average" something went wrong:')
                logging.error(f'\tr1ya == {r1ya}')
                logging.error(f'\tr2ya == {r2ya}')
                return None

        partial_for_day = [row for row in self.GA_MENSUALPARCIAL if row['FechaFiltro'] == day]

        if partial_for_day:
            partial_for_day = partial_for_day[0]
            partial = partial_for_day['Users']
            return partial/w_avg
        else:
            logging.error(f'In calculating "weighted_average" something went wrong:')
            logging.error(f'\t')
            logging.error(f'\tr1ya == {r1ya}')
            logging.error(f'\tr2ya == {r2ya}')
            logging.error(f'\t')
            logging.error(f'\tpartial_for_day == {partial_for_day}')
            logging.error(f'\t')
            return None

def main(start_date=None, end_date=None, verbose=None, debug=None):
    print(f'start_date : {start_date}')
    print(f'end_date : {end_date}')
    print(f'verbose : {verbose}')
    print(f'debug : {debug}')

    # pull tables from server
    #   connect to server
    try:
        logging.info("Connecting to database...")
        conn = pyodbc.connect(
            'Driver={SQL Server};' +
            'Server=192.168.100.18;' +
            'Database='+ 'ANALITICA' + ';' +
            'UID=python_externo;' +
            'PWD=MazzPythonNew'
        )
        cursor = conn.cursor()
    except Exception as exc:
        logging.error('Something when wrong while trying to connect to the database')
        logging.error(f'\tEXCEPTION INFORMATION:', exc_info=exc)
        raise
    
    db_tables = {}
    running_tables = {}
    conv = {
        'Users' : float,
        'Sessions' : float,
        'Pageviews' : float,
        'FechaFiltro' : date.fromisoformat
    }
    for db_table_name in db_tbls_names:
        time_start = datetime.now()

        logging.info(f'Retrieving {db_table_name}, at time {time_start}')
        try:
            curr = cursor.execute(f"SELECT Users, Sessions, Pageviews, FechaFiltro, FechaCreacion FROM {db_table_name}"
                                       f" WHERE Origen = 'RED CienRadios'"
                                       f" ORDER BY FechaFiltro ASC")
            db_tables[db_table_name] = DBTableContainer(db_table_name, curr.fetchall())
        except Exception as exc:
            logging.error(f'Exception while executing SQL statement for table {db_table_name}')
            logging.error(f'\tEXCEPTION INFORMATION:', exc_info=exc)
            raise
        #   normalize datatypes
        running_tables[db_table_name] = SortedTable(
            db_table_name,
            db_tables[db_table_name],
            conv,
            sort_func=lambda x: x['FechaFiltro']
        )
        time_end = datetime.now()

        logging.info(f'\t... got {db_table_name}, at time {time_end}. Took: {time_end - time_start}')
        logging.info(f'\tRows on Database Table {db_table_name} :: {len(db_tables[db_table_name])}')
        logging.info(f'\tRows on Running Table {db_table_name} :: {len(running_tables[db_table_name])}')
        logging.info(f'\tRunning Table {db_table_name} :: {"is" if running_tables[db_table_name].check_consistency() else "IS NOT"} consistent')

    predictor = RatioPredictor(running_tables)
 
    # -------------------------------------------------------------------------
    for predict_for_date in daterange(
        start_date, 
        end_date
    ):
        # predict_for_date = running_tables['GA_DIARIO'][-2]['FechaFiltro']

        # UKEY = date.today().strftime('%Y%m%d')
        UKEY = predict_for_date.strftime('%Y%m%d')

        Forecast_w_1y = predictor.predict(predict_for_date)
        Forecast_w_2y = predictor.predict_w(predict_for_date, 0.5)
        Forecast_w_2y_optimal = 0   # TODO -- Placeholder: predictor.predict_w(last_valid_date, w) where w better adjust for a given period
        Weight = 0.0    

        # FechaFiltro = date.today().replace(day=1) + relativedelta(months=+1, days=-1)
        FechaFiltro = (_ := predict_for_date).replace(day=1) + relativedelta(months=+1) - relativedelta(days=1)
        FechaCreacion = datetime.now().isoformat(timespec='milliseconds')
    
        if verbose:
            print(f"('{UKEY}', {Forecast_w_1y}, {Forecast_w_2y}, {Forecast_w_2y_optimal}, {Weight}, '{FechaFiltro}', '{FechaCreacion}')")

        try:
            curr = cursor.execute(f"INSERT INTO {forecast_tbl} (UKEY, Forecast_w_1y, Forecast_w_2y, Forecast_w_2y_optimal, Weight, FechaFiltro, FechaCreacion)"
                                  f" VALUES ('{UKEY}', {Forecast_w_1y}, {Forecast_w_2y}, {Forecast_w_2y_optimal}, {Weight}, '{FechaFiltro}', '{FechaCreacion}')")
            conn.commit()
        except Exception as exc:
            logging.error(f'Exception while executing SQL statement for table {forecast_tbl}')
            logging.error(f'\tEXCEPTION INFORMATION:', exc_info=exc)
            # raise

def daterange(start_date, end_date):
    current_date = start_date
    while current_date < end_date:
        yield current_date
        current_date += relativedelta(days=1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        'monthly_views_forecast',
        description='',
        epilog=''
    )

    def end_date(end_date):
        try:
            ed = date.fromisoformat(end_date)
        except ValueError as exc:
            logging.info(f'end_date = {end_date} not a valid ISO-8601 date. Defaulting to today = {date.today()}')
            ed = date.today()
        
        return ed

    parser.add_argument('-s', '--start-date', type=date.fromisoformat)
    parser.add_argument('-e', '--end-date', type=end_date)
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-d', '--debug', action='store_true')
    
    args = parser.parse_args()

    main(start_date=args.start_date,
         end_date=args.end_date,
         verbose=args.verbose,
         debug=args.debug)