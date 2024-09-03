import sqlalchemy
import urllib
import logging
import time
from modules.calendar_query_module import calendar_query
start_time = time.time()

# ------------------------------------------------------------------send to 89 server--------------------------------------

class sending_sql:

       def send_sql(final):

              quoted = urllib.parse.quote_plus("Driver={SQL Server Native Client 11.0};"
                                   "Server=10.0.0.89;"
                                   "Database=DataTeamSandbox;"
                                   "Trusted_Connection=yes;")

              engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

              # specify data types so it delivers in SQL correct. 
              final.to_sql('Workday_Employee_ADA', schema='dbo', con = engine, if_exists = 'replace', index = False,
                     dtype={'School Year': sqlalchemy.types.VARCHAR(length=10),
                            'Emp_ID': sqlalchemy.types.VARCHAR(length=10),
                            'Company': sqlalchemy.types.VARCHAR(),
                            'Location': sqlalchemy.types.VARCHAR(length = 50),
                            'Worker': sqlalchemy.types.VARCHAR(),
                            'Title': sqlalchemy.types.VARCHAR(length = 255),
              #                     'Hire_Date' : sqlalchemy.types.DateTime(),
              #                     'Term_Date': sqlalchemy.types.DateTime(),
                            'Calendar Start Date': sqlalchemy.types.Float,
                            'Calendar End Date': sqlalchemy.types.Float,
                            'LOA Days': sqlalchemy.types.INTEGER(),
                            'WTO Days': sqlalchemy.types.Float(),
                            'Total Membership': sqlalchemy.types.INTEGER(),
                            'Attendance Days': sqlalchemy.types.Float(),
                            'Attendace %' : sqlalchemy.types.Float()
                            })
              
              engine.dispose()
              how_long = time.time() - start_time
              logging.info('Data sent to SQL in {} seconds\n-----------------------'.format(how_long))


def sql_calls(region_acronym, fall_prior = None, spring_prior = None):

    #This is called for this year, overrode by fall_prior and spring_prior if they exist
    spring, fall = calendar_query.date_filter()

    #If fall prior exists then override fall variable, else pass on it. Same for spring
    if fall_prior is not None:
        fall = fall_prior
    if spring_prior is not None:
        spring = spring_prior
    
    logging.info(f'The {region_acronym} fall and spring years for the SQL query are {fall}-{spring}')
    region_cal = calendar_query.calendar_process(region_acronym, fall, spring)

    year_acronym = str(region_cal.iloc[0]['DATE_VALUE'].year) + '-' + str(region_cal.iloc[-1]['DATE_VALUE'].year)

    return(region_cal, year_acronym)