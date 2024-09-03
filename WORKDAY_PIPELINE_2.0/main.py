import warnings
import logging
import pandas as pd
import os
from datetime import date
from config import user, password

from modules.data_request_module import Data_Request
from modules.transformation_module import transformation
from modules.sending_sql_module import sending_sql
from modules.logging_metadata import *
from modules.sending_sql_module import *
from modules.process import *

warnings.simplefilter("ignore")
pd.set_option('mode.chained_assignment', None)
today = date.today().strftime("%Y/%m/%d")
today = today.replace('/','-')
today_year = today[0:4]

logger = JobLogger(process_name='Workday_Employee_ADA', 
                   job_name='Workday_Employee_ADA', 
                   job_type='python')

logging.basicConfig(filename=os.getcwd()+'\\logs\\Workday_Attendance.log', level=logging.INFO,
                   format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S',force=True)

# --------------------------Calling the Process----------------

def read_in_and_format_xml():

    logging.info('\n\n----------------New logging instance-----------------------')

    All = 'https://wd5-services1.myworkday.com/ccx/service/customreport/greendotca/ashley.ngeth/GDPS_Roster__All_Statuses__-_KM_Data__Scheduled_'
    WTO = 'https://wd5-services1.myworkday.com/ccx/service/customreport2/greendotca/ashley.ngeth/GDPS_Worker_Time_Off__Date-Ranged__-_KM_Data__Scheduled_'
    LOA = 'https://wd5-services1.myworkday.com/ccx/service/customreport/greendotca/ashley.ngeth/GDPS_LOA_-_Workers_on_Leave_-_KM_Data__Scheduled_?Organizations-id=2501%242&Include_Workers_Returned_from_Leave=1&Include_Pending_Events=0&Start_Date=2020-08-01-07%3A00&Include_Subordinate_Organizations=1'

    #Instantiate the Data_Request class
    WD = Data_Request(user, password)
    All = WD.get_report(All)
    WTO = WD.get_report(WTO) 
    LOA = WD.get_report(LOA)

    #transform the data from XML files to pandas dfs, and modify columns
    All_ = transformation.modify_all(All)
    LOA_ = transformation.modify_LOA(LOA)
    WTO_ = transformation.modify_WTO(WTO)

    transformation.singular_date_format(WTO_, 'Time Off Date')
    transformation.singular_date_format(LOA_, 'Actual Last Day')
    transformation.singular_date_format(LOA_, 'First Day')
    transformation.singular_date_format(All_, 'Term_Date')
    transformation.singular_date_format(All_, 'Hire_Date')

    return(All_, LOA_, WTO_)


#This can be expanded out to include home office admin
def main():

    All_, LOA_, WTO_ = read_in_and_format_xml()

    # If fall_prior, and spring_prior are present it subs in for fall & spring variables. If nothing is there they assume None value and the spring fall func makes it up to this SY date
    # Keep in mind these need to exist no matter what for the loop to calc prior years attendance
    fall_cal_2024, year_str_2024 = sql_calls('CA')
     
    WB = process(WTO_, LOA_, All_, fall_cal_2024, year_str_2024)

    # # # # This portion takes about 4-5 mins on the send
    sending_sql.send_sql(WB)

    return(WB)


WB = main()


try:
    main() 
    logging.info('Process was a success')
    logger.log_job('Success')
    logger.send_frame_to_SQL()
except Exception as e:        
    logging.info(f'Process failed due to the following: {e}')
    logger.log_job('Failure')
    logger.send_frame_to_SQL()