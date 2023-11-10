import sqlalchemy
import urllib
import logging
import time
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