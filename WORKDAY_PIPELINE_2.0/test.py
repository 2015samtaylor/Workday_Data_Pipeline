import warnings
import logging
import pandas as pd
import os
from datetime import date
from config import user, password

from modules.data_request_module import Data_Request
from modules.transformation_module import transformation
from modules.calendar_query_module import calendar_query
from modules.fixing_employee_calendar_module import fixing_employee_calendar
from modules.leave_of_absence_module import leave_of_absence
from modules.worker_time_off_module import worker_time_off
from modules.sending_sql_module import sending_sql
from modules.accuracy_tests_module import accuracy_tests

warnings.simplefilter("ignore")
pd.set_option('mode.chained_assignment', None)
today = date.today().strftime("%Y/%m/%d")
today = today.replace('/','-')
today_year = today[0:4]

logging.basicConfig(filename=os.getcwd()+'\\logs\\Workday_Attendance.log', level=logging.INFO,
                   format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S',force=True)

# --------------------------Calling the Process----------------

def read_in_and_format_xml():

    logging.info('\n\n----------------New logging instance-----------------------')

    terms_by_date_range = 'https://services1.myworkday.com/ccx/service/customreport2/greendot/ashley.ngeth/GDPS_Terminations_by_Date_Range_-_KM_Data__Scheduled_'
    All = 'https://services1.myworkday.com/ccx/service/customreport2/greendot/ashley.ngeth/GDPS_Roster__All_Statuses__-_KM_Data__Scheduled_'
    WTO = 'https://services1.myworkday.com/ccx/service/customreport2/greendot/ashley.ngeth/GDPS_Worker_Time_Off__Date-Ranged__-_KM_Data__Scheduled_'
    LOA = 'https://services1.myworkday.com/ccx/service/customreport/greendot/ashley.ngeth/GDPS_LOA_-_Workers_on_Leave_-_KM_Data__Scheduled_?Organizations-id=2501%242!2501%247!2501%243&Include_Workers_Returned_from_Leave=1&Include_Pending_Events=0&Start_Date=2020-08-01-07%3A00&Include_Subordinate_Organizations=1'

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

    All_ = transformation.clear_up_initial_terminations(All_)


    return(All_, LOA_, WTO_)

#Only called once, and same All_ file filtered recursively through the process function.  
All_, LOA_, WTO_ = read_in_and_format_xml()

# All_.to_csv('All_Employees.csv', index=False)
# LOA_.to_csv('Leave_of_Absence.csv', index =False)
# WTO_.to_csv('Worker_Time_Off.csv', index = False)

# --------------------------------------------------------------------------------
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


def process(acronym, WTO_, LOA_, All_, fall_cal, year_str):

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

    # Create the directory if it doesn't exist for accuracy checks on csvs
    csv_dir = os.path.join(os.getcwd(), 'csvs')
    os.makedirs(csv_dir, exist_ok=True)

    # # #get specific calendar dict, region, first_day, and last_day of a fall calendar year
    # This is restarted at every iteration of the for loop because it stems from the All_ frame, and is filtered with fall_cal
    region, calendar_dict, region_first_day, region_last_day = fixing_employee_calendar.get_specifics(fall_cal, All_)

    #Create initial mapping baseline for Calendar Start & End Dates
    region = fixing_employee_calendar.initial_mapping_calendar_start_end(region, region_first_day, calendar_dict)

    #region_original, & original_hire_dict returned here for tranparency in testing
    region, region_original, original_hire_dict = fixing_employee_calendar.generate_and_apply_dictionary(region, fall_cal, calendar_dict, 'Calendar Start Date')
    

    #map correct first and last day, figure out LOA's that are ongoing and have yet to come and correct calendar dates
    LOA_SY = leave_of_absence.map_LOA_first_last(LOA_, calendar_dict, region_first_day, region_last_day)

    #create the WB that has LOA days for given year with Calendar Start & End Date
    #This is also where EMPS are dropped based on their Original Hire Date
    WB, region_original = leave_of_absence.create_total_leave_days(LOA_SY, region, fall_cal)

    # Opportunity to catch any emps that have been dropped from the region frame
    fixing_employee_calendar.write_out_terminations(WB, region_original, acronym, year_str)

    #create an output that tells whether WTO was during LOA
    output, WTO_SY = leave_of_absence.check(LOA_SY, fall_cal, WTO_, region_first_day, region_last_day)

    #Return the WTO for this SY, with wrong days removed
    WTO_SY = worker_time_off.remove_WTO_during_LOA(output, WTO_SY)

    WB = worker_time_off.map_new_absence_days(WTO_SY, WB)
    
    #Identify instances when rehired in middle of year after a termination
    rehires = fixing_employee_calendar.locate_rehires(WB, region_first_day, region_last_day)

    #Change the Calendar Dates for rehires, and map back to the WB
    WB = fixing_employee_calendar.fix_rehire_calendar_dates(rehires, fall_cal, calendar_dict, WB)
    # WB, new_dict = fixing_employee_calendar.fix_dates_out_of_calendar(WB, fall_cal, calendar_dict, WB, 'Term_Date')
    
    WB = worker_time_off.final_wbs_modifications(WB)   
    WB = worker_time_off.mapping(acronym, WB)
    
    return(WB, fall_cal, calendar_dict, region_last_day, region_first_day)
 


# If fall_prior, and spring_prior are present it subs in for fall & spring variables. If nothing is there they assume None value and the spring fall func makes it up to this SY date
# Keep in mind these need to exist no matter what for the loop to calc prior years attendance
fall_cal_2023, year_str_2023 = sql_calls('CA')
# fall_cal_2022, year_str_2022 = sql_calls('CA', 2022, 2023)
# fall_cal_2021, year_str_2021 = sql_calls('CA', 2021, 2022)     

WB, fall_cal, calendar_dict, region_last_day, region_first_day= process('CA', WTO_, LOA_, All_, fall_cal_2023, year_str_2023)





def fix_dates_out_of_calendar(region, fall_cal, calendar_dict, what_col_fix):

        #before any processing ensure that these are the same data types
        region[what_col_fix] = pd.to_datetime(region[what_col_fix])
        fall_cal['DATE_VALUE'] = pd.to_datetime(fall_cal['DATE_VALUE'])

        #set the index on date_value to match up the errors
        fall_cal = fall_cal.set_index('DATE_VALUE')

        #append to the output list utilizing errors to match up on closest calendar day
        output_list = []

        for index, row in region.iterrows():

            # Locate Original Hire_Date, and Employee ID
            empid = row['Emp_ID']
            faulty_date = row[what_col_fix]
            
            try:
                
                idx = fall_cal.index.get_loc(faulty_date, method='nearest')  #get nearest Calendar day based on index of df frame. 
                ts = fall_cal.index[idx]

                output = (empid, ts)    #get emp_id and timestamps, turn into df
                output_list.append(output)

            except KeyError:

                print(empid)

        new_dict = pd.DataFrame(output_list, columns = ['Emp_ID', 'TimeStamp'])
        new_dict = dict(zip(new_dict['Emp_ID'], new_dict['TimeStamp']))

        # #Change the original hire date if not on the calendar, and then remap Calendar Start Date
        region[what_col_fix] = region['Emp_ID'].map(new_dict).fillna(region[what_col_fix])

        if what_col_fix == 'Term_Date':
            region['Calendar End Date'] = region[what_col_fix].map(new_dict).fillna(region['Calendar End Date'])

        elif what_col_fix == 'Hire_Date':
            region['Calendar Start Date'] = region[what_col_fix].map(new_dict).fillna(region['Calendar Start Date'])
            #Lastly have Calendar End Date, span out until the End
            region['Calendar End Date'] = len(new_dict)   
        else:
            print('Wrong column for arg what_col_fix')
        
        #Update this with the master frame
        # WB.update(region)
        return(WB)


fix_dates_out_of_calendar(WB, fall_cal, calendar_dict, 'Term_Date')