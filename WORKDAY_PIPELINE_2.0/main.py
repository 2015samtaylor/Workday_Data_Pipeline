import warnings
import logging
import pandas as pd
import os
from datetime import date
from config import user, password

from data_request_module import Data_Request
from transformation_module import transformation
from calendar_query_module import calendar_query
from fixing_employee_calendar_module import fixing_employee_calendar
from leave_of_absence_module import leave_of_absence
from worker_time_off_module import worker_time_off
from accuracy_tests_module import accuracy_tests

warnings.simplefilter("ignore")
pd.set_option('mode.chained_assignment', None)
today = date.today().strftime("%Y/%m/%d")
today = today.replace('/','-')
today_year = today[0:4]

logging.basicConfig(filename='Workday_Attendance.log', level=logging.INFO,
                   format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S',force=True)

# --------------------------Calling the Process----------------

def read_in_and_format_xml():

    logging.info('\n\n----------------New logging instance-----------------------')

    terms_by_date_range = 'https://services1.myworkday.com/ccx/service/customreport2/greendot/ashley.ngeth/GDPS_Terminations_by_Date_Range_-_KM_Data__Scheduled_'
    All = 'https://services1.myworkday.com/ccx/service/customreport2/greendot/ashley.ngeth/GDPS_Roster__All_Statuses__-_KM_Data__Scheduled_'
    WTO = 'https://services1.myworkday.com/ccx/service/customreport2/greendot/ashley.ngeth/GDPS_Worker_Time_Off__Date-Ranged__-_KM_Data__Scheduled_'
    LOA = 'https://services1.myworkday.com/ccx/service/customreport2/greendot/ashley.ngeth/GDPS_LOA_-_Workers_on_Leave_-_KM_Data__Scheduled_?Organizations!WID=13ef433eac0c108cf7f9ffdb3b27ddd4!b5818b0509f201516ff208da0802d536!13ef433eac0c108cf7fa03d77bd0dde0&Include_Workers_Returned_from_Leave=1&Include_Pending_Events=0&Start_Date=2022-07-16-07:00&Include_Subordinate_Organizations=1'

    #Instantiate the Data_Request class
    WD = Data_Request(user, password)
    termination = WD.get_report(terms_by_date_range)
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

#Only called once, and same All_ file filtered recursively through the process function.  
All_, LOA_, WTO_ = read_in_and_format_xml()

# --------------------------------------------------------------------------------

#Eventually process will be called in a loop for all regions, for all years

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

    return(region_cal)


def process(acronym, WTO_, LOA_, All_):

    #Filter all of the frames in the operation to their respected region
    if acronym == 'CA':
        filt = 'Green Dot Public Schools California'
    elif acronym == 'TN':
        filt = 'Green Dot Public Schools Tennessee'
    elif acronym == 'TX':
        filt = 'Green Dot Public Schools Southeast Texas'

    WTO_ = WTO_.loc[WTO_['Company'] == filt].reset_index(drop = True)
    LOA_ = LOA_.loc[LOA_['Company'] == filt].reset_index(drop = True)
    All_ = All_.loc[All_['Company'] == filt].reset_index(drop = True)


    #If fall_prior, and spring_prior are present it subs in for fall & spring variables. If nothing is there they assume None value and the spring fall func makes it up to this SY date
    #Keep in mind these need to exist no matter what for the loop to calc prior years attendance
    fall_cal_2023 = sql_calls(acronym)
    fall_cal_2022 = sql_calls(acronym, 2022, 2023)
    fall_cal_2021 = sql_calls(acronym, 2021, 2022)

    #with all instances of the calendar, probably going to need to call with a for loop here with an emphasis on regions, and yoy as the arguments
    fall_cals_list = [fall_cal_2023, fall_cal_2022, fall_cal_2021]

    # Create the directory if it doesn't exist for accuracy checks on csvs
    csv_dir = os.path.join(os.getcwd(), 'csvs')
    os.makedirs(csv_dir, exist_ok=True)

    WB_list = []

    for index, fall_cal in enumerate(fall_cals_list):

        # # #get specific calendar dict, region, first_day, and last_day of a fall calendar year
        # This is restarted at every iteration of the for loop because it stems from the All_ frame, and is filtered with fall_cal
        region, calendar_dict, region_first_day, region_last_day = fixing_employee_calendar.get_specifics(fall_cal, All_)

        print(region_first_day, region_last_day)

        region = fixing_employee_calendar.insert_starts_ends(region, calendar_dict, region_first_day)
        region = fixing_employee_calendar.calendar_errors(region, fall_cal, calendar_dict, 'Calendar Start Date')
        region = fixing_employee_calendar.calendar_errors(region, fall_cal, calendar_dict, 'Calendar End Date')

        # # # # # #map correct first and last day, figure out LOA's that are ongoing and have yet to come and correct calendar dates
        LOA_SY = leave_of_absence.map_LOA_first_last(LOA_, calendar_dict, region_first_day, region_last_day)
        LOA_SY.to_csv(os.path.join(csv_dir , f'{acronym}_LOA_SY_{index}.csv'))

        # # # # # #create the WB that has LOA days for given year,with Calendar Start & End Date
        WB = leave_of_absence.create_total_leave_days(LOA_SY, region, fall_cal)

        #create an output that tells whether WTO was during LOA
        output, WTO_SY = leave_of_absence.check(LOA_SY, fall_cal, WTO_, region_first_day, region_last_day)
        
        #Return the WTO for this SY, with wrong days removed
        WTO_SY = worker_time_off.remove_WTO_during_LOA(output, WTO_SY)
        WTO_SY.to_csv(os.path.join(csv_dir, f'{acronym}_WTO_SY_{index}.csv'))
    
        WB = worker_time_off.map_new_absence_days(WTO_SY, WB)
        WB.to_csv(os.path.join(csv_dir, f'{acronym}_WB_{index}.csv'))

        WB = worker_time_off.final_wbs_modifications(WB)   
        WB = worker_time_off.mapping(acronym, WB)
        WB_list.append(WB)
     
    final = pd.concat(WB_list)
    return(final)

CA = process('CA', WTO_, LOA_, All_)
TX = process('TX', WTO_, LOA_, All_)
TN = process('TN', WTO_, LOA_, All_)
