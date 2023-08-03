import argparse
import logging
import os
import decimal
import itertools as it

from datetime import datetime, date
from dateutil.relativedelta import relativedelta

import pyodbc

from Tables import *
from Predictors import *

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

    

    one_year_predictor = RatioOneYearPredictor(running_tables)
    two_year_predictor = RatioTwoYearAvgPredictor(running_tables)
    two_avgd_predictor = RatioTwoYearWghtdAvgPredictor(running_tables)
    past_mon_predictor = PastMonthPredictor(running_tables)


    # RatioTwoYearWghtdAvgPredictor need to have the w :: weight parameter adjusted so as to minimize the sum of the errors-sqrd
    
    # -------------------------------------------------------------------------
    for date_to_predict_for in daterange(
        start_date, 
        end_date
    ):
        # UKEY = date.today().strftime('%Y%m%d')
        UKEY = date_to_predict_for.strftime('%Y%m%d')

        monthly = one_year_predictor.monthly(date_to_predict_for)

        Forecast_w_1y = one_year_predictor.predict(date_to_predict_for)
        abs_err_w_1y = one_year_predictor.abs_err(date_to_predict_for)
        rel_err_w_1y = one_year_predictor.rel_err(date_to_predict_for)
        per_err_w_1y = one_year_predictor.per_err(date_to_predict_for)

        Forecast_w_2y = two_year_predictor.predict(date_to_predict_for)
        abs_err_w_2y = two_year_predictor.abs_err(date_to_predict_for)
        rel_err_w_2y = two_year_predictor.rel_err(date_to_predict_for)
        per_err_w_2y = two_year_predictor.per_err(date_to_predict_for)

        Forecast_w_2y_optimal = 0   # TODO -- Placeholder: predictor.predict_w(last_valid_date, w) where w better adjust for a given period
        abs_err_w_2y_optimal = 0
        rel_err_w_2y_optimal = 0
        per_err_w_2y_optimal = 0
        Weight = 0.0    

        # coso._historic_prev_month(date_to_predict_for)
        
        Forecast_w_past_mon = past_mon_predictor.predict(date_to_predict_for)
        abs_err_w_past_mon = past_mon_predictor.abs_err(date_to_predict_for)
        rel_err_w_past_mon = past_mon_predictor.rel_err(date_to_predict_for)
        per_err_w_past_mon = past_mon_predictor.per_err(date_to_predict_for)

        # FechaFiltro = date.today().replace(day=1) + relativedelta(months=+1, days=-1)
        FechaFiltro = (_ := date_to_predict_for).replace(day=1) + relativedelta(months=+1) - relativedelta(days=1)
        FechaCreacion = datetime.now().isoformat(timespec='milliseconds')
    
        if verbose:
            print(f"('{UKEY}', {Forecast_w_1y}, {Forecast_w_2y}, {Forecast_w_2y_optimal}, {Weight}, '{FechaFiltro}', '{FechaCreacion}')")
            print(f"(abs_err_w_1y, rel_err_w_1y) == ({abs_err_w_1y}, {rel_err_w_1y})")
            print(f"(abs_err_w_2y, rel_err_w_2y) == ({abs_err_w_2y}, {rel_err_w_2y})")

        try:
            # curr = cursor.execute(f"INSERT INTO {forecast_tbl} (UKEY, Forecast_w_1y, Forecast_w_2y, Forecast_w_2y_optimal, Weight, FechaFiltro, FechaCreacion) "
            #                       f"VALUES ('{UKEY}', {Forecast_w_1y}, {Forecast_w_2y}, {Forecast_w_2y_optimal}, {Weight}, '{FechaFiltro}', '{FechaCreacion}')")
            
            curr = cursor.execute(f"INSERT INTO {forecast_tbl} "
                                      f"(UKEY, monthly, "
                                      f"Forecast_w_1y, abs_err_w_1y, rel_err_w_1y, per_err_w_1y, " 
                                      f"Forecast_w_2y, abs_err_w_2y, rel_err_w_2y, per_err_w_2y, "
                                      f"Forecast_w_2y_optimal, abs_err_w_2y_optimal, rel_err_w_2y_optimal, per_err_w_2y_optimal, "
                                      f"Forecast_w_past_mon, abs_err_w_past_mon, rel_err_w_past_mon, per_err_w_past_mon, "
                                      f"Weight, FechaFiltro, FechaCreacion)"
                                  f"VALUES ('{UKEY}', {monthly}, "
                                      f"{Forecast_w_1y}, {abs_err_w_1y}, {rel_err_w_1y}, {per_err_w_1y}, "
                                      f"{Forecast_w_2y}, {abs_err_w_2y}, {rel_err_w_2y}, {per_err_w_2y}, "
                                      f"{Forecast_w_2y_optimal}, {abs_err_w_2y_optimal}, {rel_err_w_2y_optimal}, {per_err_w_2y_optimal}, "
                                      f"{Forecast_w_past_mon}, {abs_err_w_past_mon}, {rel_err_w_past_mon}, {per_err_w_past_mon}, "
                                      f"{Weight}, '{FechaFiltro}', '{FechaCreacion}')")
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