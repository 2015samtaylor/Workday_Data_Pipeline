import pandas as pd
import pyodbc
import logging
import datetime
from transformation_module import transformation


# ----------------------------------Calendar Class makes SQL query to get calendar, and applies to All Frame--------------

class calendar_query:
    
    def __init__(self, query):
        self.query = query
    
    def SQL_query(self):
        odbc_name = 'GD_DW'
        try:
            conn = pyodbc.connect(f'DSN={odbc_name};')
            df_SQL = pd.read_sql_query(self.query, con = conn)
            logging.info('SQL call successful - {}'.format(self.query))
            conn.close()
        except:
            logging.info('Unable to make SQL call - {}'.format(self.query))
            conn.close()
        return(df_SQL)
    
    def SQL_query_TX(self):
        odbc_name = 'GD_DW_94'
        try:
            conn = pyodbc.connect(f'DSN={odbc_name};')
            df_SQL = pd.read_sql_query(self.query, con = conn)
            logging.info('SQL call successful - {}'.format(self.query))
            conn.close()
        except:
            logging.info('Unable to make SQL call - {}'.format(self.query))
            conn.close()
        return(df_SQL)
    
    def date_filter():
            

            today = datetime.datetime.today()
            today_year = today.year
            today_month = today.month
            today_day = today.day
            next_year = today_year + 1
            prior_year = today_year - 1

            if today.month < 6:
                spring_year_end = datetime.datetime(today_year, 6, 2)
            else:
                spring_year_end = datetime.datetime(next_year, 6, 2)

            if today.month >= 6:
                fall_year_end = datetime.datetime(today_year, 12, 31)
            else:
                fall_year_end = datetime.datetime(prior_year, 12, 31)

            fall_year = fall_year_end.year
            spring_year = spring_year_end.year
            
            return(spring_year, fall_year)


    def calendar_process(region_acronym, fall, spring):
     
        #make an instance of the Calendar Class and establish the regions calendar
        
        if region_acronym  == 'CA':
  
            CA = calendar_query('''
            SELECT [SCHOOLID] ,[DATE_VALUE] ,[MEMBERSHIPVALUE] 
            FROM [PowerschoolStaged].[dbo].[CALENDAR_DAY] 
            where DATE_VALUE >'{}-08-01' and DATE_VALUE <= '{}-07-01'
            and SCHOOLID=124016 and MEMBERSHIPVALUE=1  order by DATE_VALUE
            '''.format(fall, spring))
            region_cal = CA.SQL_query()


        elif region_acronym == 'TN':

            TN = calendar_query('''
            SELECT [SCHOOLID] ,[DATE_VALUE] ,[MEMBERSHIPVALUE] 
            FROM [PowerschoolStaged].[dbo].[CALENDAR_DAY] 
            where DATE_VALUE >'{}-08-01' and DATE_VALUE <= '{}-07-01'
            and SCHOOLID=8055 and MEMBERSHIPVALUE=1  order by DATE_VALUE
            '''.format(fall, spring))

            region_cal = TN.SQL_query()

        elif region_acronym == 'TX':

            TX=calendar_query('''
            SELECT DISTINCT [STUDENT_CAL_DATE] AS DATE_VALUE, [STUDENT_CAL_DATE_TYPE], [STUDENT_CAL_FISCAL_YEAR] 
            FROM [Frontline].[dbo].[STUDENT_CALENDAR_DATE] 
            WHERE STUDENT_CAL_DATE_TYPE = 'Instructional'
            AND STUDENT_CAL_DATE > '{}-08-01'
            AND STUDENT_CAL_DATE <= '{}-07-01' ORDER BY STUDENT_CAL_DATE
            '''.format(fall, spring))

            region_cal = TX.SQL_query_TX()

        return(region_cal)
    

#probably going to need to move full_name to fixing_start_ends_module










