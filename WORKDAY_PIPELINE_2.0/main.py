import warnings
import logging
import pandas as pd
import datetime
from config import user, password

from data_request_module import Data_Request
from transformation_module import data_transformation

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
    

# --------------------------Calling the Process----------------

if __name__ == '__main__':
    print("Main script is running")
    #get year ends
    spring, fall = date_filter()

    #Instantiate the Data_Request class
    WD = Data_Request(user, password)
    termination = WD.get_report(terms_by_date_range)
    All = WD.get_report(All)
    WTO = WD.get_report(WTO) 
    LOA = WD.get_report(LOA)

    #transform the data from XML files to pandas dfs, and modify columns
    All_ = data_transformation.modify_all(All)
    LOA_ = data_transformation.modify_LOA(LOA)
    WTO_ = data_transformation.modify_WTO(WTO)

    data_transformation.singular_date_format(WTO_, 'Time Off Date')
    data_transformation.singular_date_format(LOA_, 'Actual Last Day')
    data_transformation.singular_date_format(WTO_, 'First Day')
    data_transformation.singular_date_format(WTO_, 'Term_Date')
    data_transformation.singular_date_format(WTO_, 'Hire_Date')





