# Tables.py

import logging
import itertools as it

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