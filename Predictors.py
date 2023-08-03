# Predictors.py

import logging

from datetime import datetime, date
from dateutil.relativedelta import relativedelta

class Predictor:
    def __init__(self, tables):
        self.tables = tables

    def predict(self, day=date.today()):
        return self._predict(day)
    
    def historic(self, day):
        return self._historic(day)
    
    def monthly(self, day):
        return self._historic_monthly(day)['Users']
    
    def abs_err(self, day):
        monthly = self.monthly(day)
        
        return (self.predict(day) - monthly) if monthly != None else None

    def rel_err(self, day):
        abs_err = self.abs_err(day)
        
        return (abs_err/self.monthly(day)) if abs_err != None else None
    
    def per_err(self, day):
        return 100 * self.rel_err(day)

    def _historic_partial(self, day):
        partial = [row for row in self.tables['GA_MENSUALPARCIAL'] if row['FechaFiltro'] == day]
        
        if len(partial) == 1:
            return partial[0]
        else:
            logging.error(f'Something went wrong when retrieving historic values')
            logging.error(f'\t(day, month) = ({day}, {Predictor._month_from_day(day)})')
            logging.error(f'\t(len(partial), partial) == ({len(partial)}, {partial})')
            return None
    
    def _historic_monthly(self, day):
        monthly = [row for row in self.tables['GA_MENSUAL'] if row['FechaFiltro'] == Predictor._month_from_day(day)]
        
        if len(monthly) == 1:
            return monthly[0]
        else:
            logging.error(f'Something went wrong when retrieving historic values')
            logging.error(f'\t(day, month) = ({day}, {Predictor._month_from_day(day)})')
            logging.error(f'\t(len(monthly), monthly) == ({len(monthly)}, {monthly})')
            return None

    @staticmethod
    def _days_to_end_of_month(day):
        ldotm = day.replace(day = 1) + relativedelta(months = +1, days = -1)     # last_day_of_the_month
        dteom = relativedelta(ldotm, day)   # days_to_end_of_month

        return dteom

    @staticmethod
    def _month_from_day(day):
        return day.replace(day=1) + relativedelta(months=+1, days=-1)

class RatioPredictor(Predictor):
    def _ratio(self, day):
        partial, monthly = self._historic_partial(day), self._historic_monthly(day)

        if partial and monthly:
            return partial['Users']/monthly['Users']
        else:
            logging.error(f'Unable to calculate partial_montly to monthly ratio')
            logging.error(f'\tday == {day}')
            logging.error(f'\t')
            return None

class RatioTwoYearWghtdAvgPredictor(RatioPredictor):
    def _predict_w_2y_weighted_average(self, day, weight):
        r1ya = self._ratio(day - relativedelta(years=1))    # ratio 1 year ago
        r2ya = self._ratio(day - relativedelta(years=2))    # ratio 2 year ago

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

        partial_for_day = [row for row in self.tables['GA_MENSUALPARCIAL'] if row['FechaFiltro'] == day]

        if partial_for_day:
            partial_for_day = partial_for_day[0]
            partial = partial_for_day['Users']
            return partial/w_avg
        else:
            logging.error(f'In calculating "weighted_average" something went wrong:')
            logging.error(f'\t(r1ya, r2ya) == ({r1ya}, {r2ya})')
            logging.error(f'\tpartial_for_day == {partial_for_day}')
            logging.error(f'\t')
            return None

class RatioTwoYearAvgPredictor(RatioTwoYearWghtdAvgPredictor):
    def _predict(self, day):
        return self._predict_w_2y(day)
    
    def _predict_w_2y(self, day):
        return self._predict_w_2y_weighted_average(day, 0.5)

class RatioOneYearPredictor(RatioTwoYearWghtdAvgPredictor):
    def _predict(self, day):
        return self._predict_w_1y(day)

    def _predict_w_1y(self, day):
        return self._predict_w_2y_weighted_average(day, 1.0)

class PastMonthPredictor(Predictor):
    def _predict(self, day):
        return self._predict_w_prev_month(day)
    
    def _predict_w_prev_month(self, day):
        partial_for_day = [row for row in self.tables['GA_MENSUALPARCIAL'] if row['FechaFiltro'] == day]
        
        if len(partial_for_day) > 1:
            logging.warning('BOILERPLATE...')

        return partial_for_day[0]['Users'] * self._ratio_prev_month(day)

    def _ratio_prev_month(self, day):
        partial_prev_month = self._partial_prev_month(day)
        monthly_prev_month = self._monthly_prev_month(day)
        
        if (partial_prev_month != None) and (monthly_prev_month != None):
            return partial_prev_month['Users'] / monthly_prev_month['Users']
        else:
            logging.error('BOILERPLATE...')
            return None
        
    def _partial_prev_month(self, day):
        prev_month_day = PastMonthPredictor._prev_month_day(day)

        partial_prev_month = [row for row in self.tables['GA_MENSUALPARCIAL'] if row['FechaFiltro'] == prev_month_day]        
        
        return partial_prev_month[0] if partial_prev_month != None else None

    def _monthly_prev_month(self, day):
        prev_month_day = PastMonthPredictor._prev_month_day(day)

        monthly_prev_month = [row for row in self.tables['GA_MENSUAL'] if row['FechaFiltro'] == Predictor._month_from_day(prev_month_day)]

        return monthly_prev_month[0] if monthly_prev_month != None else None

    @staticmethod
    def _prev_month_day(day):
        dteom = PastMonthPredictor._days_to_end_of_month(day)   # days_to_end_of_month
        prev_month_day = day.replace(day=1) - relativedelta(days=+1) - dteom

        return prev_month_day
        