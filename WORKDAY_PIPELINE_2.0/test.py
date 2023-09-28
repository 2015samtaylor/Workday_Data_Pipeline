import pandas as pd
import requests
import xmltodict
import json
import pyodbc
from datetime import date
import warnings
import urllib
import sqlalchemy
from config import user, password
import time
import datetime
import logging
import sys

start_time = time.time()
today = date.today().strftime("%Y/%m/%d")
today = today.replace('/','-')
today_year = today[0:4]

warnings.simplefilter("ignore")
pd.set_option('mode.chained_assignment', None)

logging.basicConfig(filename='Workday_Teacher_Attendance.log', level=logging.INFO,
                   format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S',force=True)

terms_by_date_range = 'https://services1.myworkday.com/ccx/service/customreport2/greendot/ashley.ngeth/GDPS_Terminations_by_Date_Range_-_KM_Data__Scheduled_'
All = 'https://services1.myworkday.com/ccx/service/customreport2/greendot/ashley.ngeth/GDPS_Roster__All_Statuses__-_KM_Data__Scheduled_'
WTO = 'https://services1.myworkday.com/ccx/service/customreport2/greendot/ashley.ngeth/GDPS_Worker_Time_Off__Date-Ranged__-_KM_Data__Scheduled_'
LOA = 'https://services1.myworkday.com/ccx/service/customreport2/greendot/ashley.ngeth/GDPS_LOA_-_Workers_on_Leave_-_KM_Data__Scheduled_?Organizations!WID=13ef433eac0c108cf7f9ffdb3b27ddd4!b5818b0509f201516ff208da0802d536!13ef433eac0c108cf7fa03d77bd0dde0&Include_Workers_Returned_from_Leave=1&Include_Pending_Events=0&Start_Date=2022-07-16-07:00&Include_Subordinate_Organizations=1'

# --------------------------------------------create date_filter to dynamically refer YoY---------------------

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
        
        return(spring_year_end, fall_year_end)
    
spring, fall = date_filter()

# ----------------------------------

class Data_Request:
    
    def __init__(self, user, password):
        self.user = user
        self.password = password
    
# ---------------------------------method to authenticate and get a report--------------------------------
    def get_report(self, link):
        
        session = requests.Session()
        session.auth = (self.user, self.password)
        
        auth = session.post(link)
        response = session.get(link)
        
        if response.status_code == 200:
            logging.info('{} \n status_code-{}\n'.format(link,response.status_code))
            doc = xmltodict.parse(response.content)
            doc = json.dumps(doc)
            doc = json.loads(doc)
            doc = doc['wd:Report_Data']['wd:Report_Entry']
            return(doc)
        else:
            raise Exception(logging.info('Failed to get Workday Reports'))

# ------------------------------------call on reports via WD instantiated object-------------------------------------------------


WD = Data_Request(user, password)

termination = WD.get_report(terms_by_date_range)
# All = WD.get_report(All)
# WTO = WD.get_report(WTO) 
# LOA = WD.get_report(LOA)

# print(All)

print(termination)


# ..\Anaconda3
# ..\Anaconda3\scripts
# ..\Anaconda3\Library\bin

# #to add
# C:\Users\samuel.taylor\Anaconda3\Scripts
# C:\Users\samuel.taylor\Anaconda3
# C:\Users\samuel.taylor\Anaconda3\Library\bin