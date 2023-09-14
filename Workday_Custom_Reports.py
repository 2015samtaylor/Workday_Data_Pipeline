import pandas as pd
import requests
import xmltodict
import json
import pyodbc
from datetime import date
import warnings
import urllib
import sqlalchemy
from workday_data_ADA.config import user, password
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
All = WD.get_report(All)
WTO = WD.get_report(WTO) 
LOA = WD.get_report(LOA)

# -----------------------------------transform XML documents to Pandas DataFrames-----------------------------------


class data_transformation:
    
    def transform(doc):
        output = []
        i = 0
        while i < len(doc):
            doc_sub = pd.DataFrame(doc[i])
            doc_sub.reset_index(inplace = True)
            doc_sub = doc_sub.drop(1)
            doc_sub.drop(columns = ['index'], inplace = True)
            output.append(doc_sub)
            i += 1
    
        
        final = pd.concat(output)
         
        #remove 'WD' from columns    
        cols_list = list(final.columns)

        new_col_list = []
        for cols in cols_list:
            new_col_list.append(cols[3:])

        final.columns = new_col_list

        return(final)
    
# ----------------------------------------------------iterate through JSON file, and send to Pandas Dataframe------------
    
    def get_LOA():
    
        total_emps = len(LOA)

        final = []

        i = 0
        while i < total_emps:
            emp_id = LOA[i]['wd:Worker_group']['wd:Emp_ID']
            company = LOA[i]['wd:Worker_group']['wd:Company']['@wd:Descriptor']
            hire_date = LOA[i]['wd:Worker_group']['wd:Hire_Date']
            location = LOA[i]['wd:Worker_group']['wd:location']['@wd:Descriptor']
            manager = LOA[i]['wd:Worker_group']['wd:Manager']['@wd:Descriptor']
            emp_name = LOA[i]['wd:Worker']['@wd:Descriptor']
            first_day = LOA[i]['wd:First_Day']
            est_last_day = LOA[i]['wd:Estimated_Last_Day']
            
            try:
                last_day = LOA[i]['wd:Last_Day_of_Work']
            
            except KeyError:
                last_day = ''

            try:
                act_last_day = LOA[i]['wd:Actual_Last_Day']


            except KeyError:
                act_last_day = ''
                

            total_days = LOA[i]['wd:Total_Days']
            units_requested = LOA[i]['wd:Units_Requested']
            unit_of_time = LOA[i]['wd:Unit_of_Time']['@wd:Descriptor']
            business_title = LOA[i]['wd:Worker_group']['wd:businessTitle']

            instance = [emp_id, emp_name, company, last_day, first_day, est_last_day, act_last_day, total_days, units_requested, unit_of_time, hire_date, location, manager, business_title ]
            final.append(instance)
            i += 1
        return(final)
    
#  -----------------------------------------------iterate through JSON file, and send to Pandas Dataframe------------ 
    
    def get_WTO():
    
        # get total employees
        total_emps = len(WTO)

        final = []
        # hang on while loop until gone through all emps
        i = 0
        while i < total_emps:
            one_emp = WTO[i]['wd:Time_Off_Completed_Details_group']
            emp_id = WTO[i]['wd:Worker']['wd:ID'][1]['#text']
            worker_status = WTO[i]['wd:Worker_Status']
            emp_name = WTO[i]['wd:Worker']['@wd:Descriptor']
            hire_date = WTO[i]['wd:Hire_Date']
            job_profile = WTO[i]['wd:Job_Profile']['@wd:Descriptor']
            company = WTO[i]['wd:Company']['@wd:Descriptor']
            school = WTO[i]['wd:location']['@wd:Descriptor']
            sum_of_hours = WTO[i]['wd:Sum_of_Hours']
            try:
                termination = WTO[i]['wd:termination_date']
            except KeyError:
                termination = ''

            j = 0
            while j < len(one_emp):

                try:
                    Time_Off_Type = one_emp[j]['wd:Time_Off_Type_for_Time_Off_Entry']['@wd:Descriptor']

                except KeyError:
                    Time_Off_Type = one_emp['wd:Time_Off_Type_for_Time_Off_Entry']['@wd:Descriptor']

                try:
                    Time_Off_Date = one_emp[j]['wd:date']

                except KeyError:
                    Time_Off_Date = one_emp['wd:date']  # see if there are duplicates. 

                try:
                    Total_Units = one_emp[j]['wd:Total_Units']

                except KeyError:

                    Total_Units = one_emp['wd:Total_Units']


                j +=1

                instance = [i, emp_name, worker_status, emp_id, hire_date, termination, job_profile, company, school, Time_Off_Date, Time_Off_Type, Total_Units, sum_of_hours ]
                final.append(instance)

            i += 1

        return(final)



def modify_all():
    All_ = data_transformation.transform(All)
    All_['Term_Date'] = All_['Term_Date'].str[0:10]
    All_['Hire_Date'] = All_['Hire_Date'].str[0:10]
#     All_ = All_.loc[All_['Title'].str.contains('Teacher')]
    All_.reset_index(drop = True, inplace = True)
    return(All_)

All_ = modify_all()

def modify_LOA():
    final = data_transformation.get_LOA()
    LOA_ = pd.DataFrame(final, columns = ['Employee ID','Worker','Company','Last Day of Work','First Day','Estimated Last Day','Actual Last Day','Total Days','Units Requested','Unit of Time','Hire Date','Location','Manager','Business Title - Current'])
#     LOA_ = LOA_.loc[LOA_['Business Title - Current'].str.contains('Teacher')]
    LOA_.reset_index(drop = True, inplace = True)
    LOA_['First Day'] = LOA_['First Day'].str[0:10]
    LOA_['Actual Last Day'] = LOA_['Actual Last Day'].str[0:10]
    LOA_.reset_index(drop = True, inplace = True)
    return(LOA_)
    
LOA_ = modify_LOA()


def modify_WTO():
    final = data_transformation.get_WTO()
    WTO_ = pd.DataFrame(final, columns = ['employee count','Worker', 'Worker Status','Employee ID',  'Hire Date', 'Termination Date', 'Job Profile', 'Company', 'Location', 'Time Off Date', 'Time Off Type for Time Off Entry', 'Total Units', 'Sum of Hours'])
    WTO_ = WTO_.drop_duplicates()
    WTO_['Total Units'] = WTO_['Total Units'].astype(float)
#     WTO_ = WTO_.loc[WTO_['Job Profile'].str.contains('Teacher')]
    WTO_.reset_index(drop = True, inplace = True)
    WTO_['Time Off Date'] = WTO_['Time Off Date'].str[0:10]
    return(WTO_)
    
WTO_ = modify_WTO()


# -----------------------------------------------------------change datetime columns to singular schema------------------


def singular_date_format(frame, col_name):

    frame[col_name] = pd.to_datetime(frame[col_name], format='%Y-%m-%d')

    date_list = []
    for dates in frame[col_name]:
        output = dates.replace(minute=0, second = 0, microsecond = 0)
        date_list.append(output)

    frame[col_name] = date_list
    

singular_date_format(WTO_, 'Time Off Date')
singular_date_format(LOA_, 'Actual Last Day')
singular_date_format(LOA_, 'First Day')
singular_date_format(All_, 'Term_Date')
singular_date_format(All_, 'Hire_Date')


# ----------------------------------Calendar Class makes SQL query to get calendar, and applies to All Frame--------------

class Calendar:
    
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
    
    # make sql query, and zip together the index & date value
    
    def calendar_zip(most_recent):
        most_recent.reset_index(inplace = True)
        most_recent['index'] = most_recent['index'] + 1
        singular_date_format(most_recent, 'DATE_VALUE')
        most_recent = most_recent[['index', 'DATE_VALUE']]
        most_recent = dict(zip(most_recent['DATE_VALUE'], most_recent['index']))
        return(most_recent)
    
   # bring in Calendar Start / End Date. Drop rows where emp has been terminated prior to the school year.  
   # IF emp has not been terminated in the current School Year, then the End Date will default to most up to date. 
   # If an EMP was fired today, it will not register on attendance until the next day. 

    def fix_calendar_starts_ends(master_frame, zip_frame, sql_frame, neutral_col, apply_col, first_day):

            if neutral_col == 'Hire_Date':

                master_frame['Calendar Start Date'] = master_frame['Hire_Date'].map(zip_frame).fillna('Error')
                master_frame.loc[master_frame['Hire_Date'] < first_day, 'Calendar Start Date'] = 1

            elif neutral_col == 'Term_Date':

                master_frame.drop(master_frame.loc[master_frame['Term_Date'] < first_day].index, inplace = True)
                master_frame['Term_Date'].replace({pd.NaT: 'Employed'}, inplace=True)
                master_frame['Calendar End Date'] = master_frame['Term_Date'].map(zip_frame).fillna('Error')
                master_frame.loc[master_frame['Term_Date'] == 'Employed', 'Calendar End Date'] = len(zip_frame)
            else:
                print('Not a valid column')

            # locate withstanding errors differing from 1. 
            e = master_frame.loc[master_frame[apply_col] == 'Error']

            # set index of CA_cal frame to 'DATE_VALUE' in order to query the closest date
            df = sql_frame.set_index('DATE_VALUE')

            output_list = []
            for i in range(len(e)):
                faulty_date = e.iloc[i][neutral_col]   #locate faulty hire date, that is not on given calendar. 
                emp_ID = e.iloc[i]['Emp_ID']
                idx = df.index.get_loc(faulty_date, method='nearest')  #get nearest day based on index of df frame. 

                ts = df.index[idx]
                output = (emp_ID, ts)
                output_list.append(output)

            outliers = pd.DataFrame(output_list, columns = ['Emp_ID', 'TimeStamp'])
            outliers_zip = dict(zip(outliers['Emp_ID'], outliers['TimeStamp']))

            # map the proper Hire Date for the employees that hire dates on weekends. 
            master_frame[neutral_col] = master_frame['Emp_ID'].map(outliers_zip).fillna(master_frame[neutral_col])

            # map the proper Calendar Start Date, based on the new Hire Dates. 
            master_frame[apply_col] = master_frame[neutral_col].map(zip_frame).fillna(master_frame[apply_col])
            
            if apply_col == 'Calendar End Date':
                #Catch instances where Subs are rehired after term dates, and give new Calendar End Date. 
                master_frame.loc[master_frame['Calendar Start Date'] > master_frame['Calendar End Date'], 'Calendar End Date'] = len(zip_frame)
            else:
                pass

            return(master_frame)
    
    def map_LOA_frames(master_frame, zip_frame, sql_frame, neutral_col, apply_col, first_day):

        if neutral_col == 'First Day':

            master_frame.drop(master_frame.loc[master_frame['Actual Last Day'] < first_day].index, inplace = True)  # Drop all days where the Last Day of Leave is before the start of the SY. 
            master_frame['Calendar Start Date'] = master_frame['First Day'].map(zip_frame).fillna('Error')   # Map Calendar Last Day - See if First Day is on Calendar
            master_frame.loc[master_frame['First Day'] < first_day, 'Calendar Start Date'] = 1   # Now locate 'First Day' values that were before First Day, and give a Calendar Start Date of 1.

        elif neutral_col == 'Actual Last Day':

            master_frame.drop(master_frame.loc[master_frame['Actual Last Day'] < first_day].index, inplace = True)
            master_frame['Calendar End Date'] = master_frame['Actual Last Day'].map(zip_frame).fillna('Error')
            master_frame['Actual Last Day'].replace({pd.NaT: 'Ongoing'}, inplace=True)
            master_frame.loc[master_frame['Actual Last Day'] == 'Ongoing', 'Calendar End Date'] = len(zip_frame)
            master_frame['Actual Last Day'].replace({'Ongoing': pd.NaT}, inplace = True)

        else:
            print('Not a valid column')


        # locate withstanding errors differing from 1. 
        e = master_frame.loc[master_frame[apply_col] == 'Error']

        # set index of CA_cal frame to 'DATE_VALUE' in order to query the closest date
        df = sql_frame.set_index('DATE_VALUE')

        output_list = []
        for i in range(len(e)):
            faulty_date = e.iloc[i][neutral_col]   #locate faulty First Day, that is not on given calendar. 
            emp_ID = e.iloc[i]['Employee ID']
            day = e.iloc[i]['First Day']
            idx = df.index.get_loc(faulty_date, method='nearest')  #get nearest day based on index of df frame. 

            ts = df.index[idx]
            output = (emp_ID, ts, day)
            output_list.append(output)

        # A PRIMARY KEY IS MADE HERE BECAUSE OF REPEAT LOA's UNDER ONE ID
        outliers = pd.DataFrame(output_list, columns = ['Emp_ID', 'TimeStamp', 'First Day'])
        outliers['PK'] = outliers['Emp_ID'] +  ' ' + outliers['First Day'].astype(str)
        outliers_zip = dict(zip(outliers['PK'], outliers['TimeStamp']))

        # # map the proper Hire Date for the employees that hire dates on weekends. 
        master_frame['PK'] = master_frame['Employee ID'] +  ' ' + master_frame['First Day'].astype(str)
        master_frame[neutral_col] = master_frame['PK'].map(outliers_zip).fillna(master_frame[neutral_col])

        # # map the proper Calendar Start Date, based on the new Hire Dates. 
        master_frame[apply_col] = master_frame[neutral_col].map(zip_frame).fillna(master_frame[apply_col])

        return(master_frame)
    
    
    def create_total_leave_days(master_frame, all_frame):
        master_frame['Total Leave Days'] = master_frame['Calendar End Date'] - master_frame['Calendar Start Date'] + 1
        #     frame['Total Membership Days'] = (frame['Calendar Last Day'] - frame['Calendar First Day']) + 1
        total_total = dict(master_frame.groupby(['Employee ID'])['Total Leave Days'].sum())

        all_frame['LOA Days'] = all_frame['Emp_ID'].map(total_total).fillna(0)
        all_frame['School Year'] = str(fall.year) + '-' + str(fall.year +1)
        all_frame = all_frame[['School Year', 'Emp_ID', 'Company', 'Location', 'Worker', 'Title' ,'Hire_Date', 'Term_Date', 'Calendar Start Date', 'Calendar End Date', 'LOA Days']]
        return(all_frame)

        
    
    
# --------------------------make queries, create calendars, filter for regions, create calendar dates, drop old terminations-------
    
CA = Calendar('''
SELECT [SCHOOLID] ,[DATE_VALUE] ,[MEMBERSHIPVALUE] 
FROM [PowerschoolStaged].[dbo].[CALENDAR_DAY] 
where DATE_VALUE >'{}-08-01' and DATE_VALUE <= '{}'
and SCHOOLID=124016 and MEMBERSHIPVALUE=1  order by DATE_VALUE
'''.format(fall.year, today))
CA_cal = CA.SQL_query()

TN = Calendar('''
SELECT [SCHOOLID] ,[DATE_VALUE] ,[MEMBERSHIPVALUE] 
FROM [PowerschoolStaged].[dbo].[CALENDAR_DAY] 
where DATE_VALUE >'{}-08-01' and DATE_VALUE <= '{}'
and SCHOOLID=8055 and MEMBERSHIPVALUE=1  order by DATE_VALUE
'''.format(fall.year, today))
TN_cal = TN.SQL_query()

TX=Calendar('''
SELECT DISTINCT [STUDENT_CAL_DATE] AS DATE_VALUE, [STUDENT_CAL_DATE_TYPE], [STUDENT_CAL_FISCAL_YEAR] 
FROM [Frontline].[dbo].[STUDENT_CALENDAR_DATE] 
WHERE STUDENT_CAL_DATE_TYPE = 'Instructional'
AND STUDENT_CAL_DATE > '{}-08-01'
AND STUDENT_CAL_DATE <= '{}' ORDER BY STUDENT_CAL_DATE
'''.format(fall.year, today))
TX_cal = TX.SQL_query_TX()

# ----------------------------------------------------------create calendars, establish regions for All Rosters-------

CA_cal_zip = Calendar.calendar_zip(CA_cal)
TN_cal_zip = Calendar.calendar_zip(TN_cal)
TX_cal_zip = Calendar.calendar_zip(TX_cal)

try:
    CA_first_day = CA_cal.loc[CA_cal['DATE_VALUE'].dt.month == 8].iloc[0]['DATE_VALUE']
    TN_first_day = TN_cal.loc[TN_cal['DATE_VALUE'].dt.month == 8].iloc[0]['DATE_VALUE']
    TX_first_day = TX_cal.loc[TX_cal['DATE_VALUE'].dt.month == 8].iloc[0]['DATE_VALUE']
except AttributeError:
    logging.info('First day of {}-{} School Year has not begun \n-----------------------'.format(fall.year, fall.year + 1))
    
    

All_CA = All_.loc[All_['Company'] == 'Green Dot Public Schools California']
All_TN = All_.loc[All_['Company'] == 'Green Dot Public Schools Tennessee']
All_TX = All_.loc[All_['Company'] == 'Green Dot Public Schools Southeast Texas']

# -----------------------------------------------------------call on Calendar Class to Fix Start/End Dates-------

All_CA = Calendar.fix_calendar_starts_ends(All_CA, CA_cal_zip, CA_cal, 'Hire_Date', 'Calendar Start Date', CA_first_day)
All_CA = Calendar.fix_calendar_starts_ends(All_CA, CA_cal_zip, CA_cal, 'Term_Date', 'Calendar End Date', CA_first_day)

All_TN = Calendar.fix_calendar_starts_ends(All_TN, TN_cal_zip, TN_cal, 'Hire_Date', 'Calendar Start Date', TN_first_day)
All_TN = Calendar.fix_calendar_starts_ends(All_TN, TN_cal_zip, TN_cal, 'Term_Date', 'Calendar End Date', TN_first_day)

All_TX = Calendar.fix_calendar_starts_ends(All_TX, TX_cal_zip, TX_cal, 'Hire_Date', 'Calendar Start Date', TX_first_day)
All_TX = Calendar.fix_calendar_starts_ends(All_TX, TX_cal_zip, TX_cal, 'Term_Date', 'Calendar End Date', TX_first_day)

# ------------------------------------establish regions for LOA's fix start/end dates for individual instances
# create workbooks for each region. 

# filter for LOAs by region
LOA_TN = LOA_.loc[LOA_['Company'] == 'Green Dot Public Schools Tennessee']
LOA_CA = LOA_.loc[LOA_['Company'] == 'Green Dot Public Schools California']
LOA_TX = LOA_.loc[LOA_['Company'] == 'Green Dot Public Schools Texas']

# Call on function to properly map Start and End Dates for instances of LOA
LOA_CA_new = Calendar.map_LOA_frames(LOA_CA, CA_cal_zip, CA_cal, 'First Day', 'Calendar Start Date', CA_first_day)
LOA_CA_new = Calendar.map_LOA_frames(LOA_CA, CA_cal_zip, CA_cal, 'Actual Last Day', 'Calendar End Date', CA_first_day)

LOA_TN_new = Calendar.map_LOA_frames(LOA_TN, TN_cal_zip, TN_cal, 'First Day', 'Calendar Start Date', TN_first_day)
LOA_TN_new = Calendar.map_LOA_frames(LOA_TN, TN_cal_zip, TN_cal, 'Actual Last Day', 'Calendar End Date', TN_first_day)

LOA_TX_new = Calendar.map_LOA_frames(LOA_TX, TX_cal_zip, TX_cal, 'First Day', 'Calendar Start Date', TX_first_day)
LOA_TX_new = Calendar.map_LOA_frames(LOA_TX, TX_cal_zip, TX_cal, 'Actual Last Day', 'Calendar End Date', TX_first_day)

WB_CA = Calendar.create_total_leave_days(LOA_CA_new, All_CA)
WB_TN = Calendar.create_total_leave_days(LOA_TN_new, All_TN)
WB_TX = Calendar.create_total_leave_days(LOA_TX_new, All_TX)

# # ----------------------------------loop through employees LOA, and WTO,identify where WTO was taken during LOA's and remove---

# LOA frame will change with each script


CA_output = []
TN_output = []
TX_output = []
def loop_through_emps(emp, frame, cal):
        
    j = 0
    
    Emp_ID_list = list(frame['Employee ID'].unique())
    LOA_sub = frame.loc[frame['Employee ID'] == Emp_ID_list[emp]].reset_index(drop = True)
    WTO_sub = WTO_.loc[WTO_['Employee ID'] == Emp_ID_list[emp]].reset_index(drop = True)
    Days_Off = WTO_sub['Time Off Date']

    while j < len(LOA_sub):
        
        i = 0
        while i < len(Days_Off):                         # Going through the instances of WTO days. 

            First_Day_LOA = LOA_sub['First Day'][j]
            Last_Day_LOA = LOA_sub['Actual Last Day'][j]
            Emp_ID = LOA_sub['Employee ID'][j]
            WTO_day = WTO_sub['Time Off Date'][i]
            
            NaT_test = pd.isnull(Last_Day_LOA)
            
            if NaT_test == True:
                Last_Day_LOA = cal.iloc[-1]['DATE_VALUE']

            vacation = (First_Day_LOA <= WTO_day) & (Last_Day_LOA >= WTO_day )

            out = [Emp_ID, First_Day_LOA, WTO_day, Last_Day_LOA, vacation]
            if frame.equals(LOA_CA):
                CA_output.append(out)
            elif frame.equals(LOA_TN):
                TN_output.append(out)
            elif frame.equals(LOA_TX):
                TX_output.append(out)
            else:
                print('Error with the input frame')

            i += 1

        j += 1
        
# # # -------------------------------------calling on all employees to see if their Leave of Absences were during days off
# IF the employee is currently on LOA the Last Day LOA will default to today. 

def check(frame, cal):
    Emp_ID = list(frame['Employee ID'].unique())

    i = 0 

    while i < len(Emp_ID):
        loop_through_emps(i, frame, cal)
        i += 1
        
    
    if frame.equals(LOA_CA):
        out = pd.DataFrame(CA_output, columns = ['Employee ID', 'First Day LOA', 'WTO Day', 'Last Day LOA', 'Status']) 
    elif frame.equals(LOA_TN):
        out = pd.DataFrame(TN_output, columns = ['Employee ID', 'First Day LOA', 'WTO Day', 'Last Day LOA', 'Status']) 
    elif frame.equals(LOA_TX):
        out = pd.DataFrame(TX_output, columns = ['Employee ID', 'First Day LOA', 'WTO Day', 'Last Day LOA', 'Status']) 
    else:
        print('Error with the input frame')
    
    return(out)

CA_output = check(LOA_CA_new, CA_cal)
TN_output = check(LOA_TN_new, TN_cal)
TX_output = check(LOA_TX_new, TX_cal)

# # -----------------------------------------------function to remove WTO days during a Leave of Absence----------


def remove_WTO_during_LOA(frame):
    # identify days where time off days are during leave of Absence. Outliers variable holds Emp ID & WTO Day to remove
    during_LOA = frame.loc[frame['Status'] == True]
    during_LOA.reset_index(drop = True, inplace = True)
    outliers = pd.DataFrame(list(zip(during_LOA['Employee ID'], during_LOA['WTO Day'])), columns = ['Employee ID', 'WTO Day'])

    # outer join, and then filtering for left only to retain day WTO that was not during LOA. 
    indicators = pd.merge(WTO_, outliers, how = 'outer', left_on=['Employee ID', 'Time Off Date'], right_on = ['Employee ID', 'WTO Day'], indicator = True )
    indicators = indicators.loc[indicators['_merge'] == 'left_only']
    indicators.drop(columns = ['WTO Day', '_merge'],inplace = True)
    indicators.reset_index(drop = True, inplace = True)
    
    
    #Filter that only gets WTO for this school year and seperates the regions

    if frame.equals(TN_output):
        indicators = indicators.loc[indicators['Company'] == 'Green Dot Public Schools Tennessee']
        indicators = indicators.loc[indicators['Time Off Date'] > TN_cal.iloc[0]['DATE_VALUE']]
    
    elif frame.equals(CA_output):
                                    
        indicators = indicators.loc[indicators['Company'] == 'Green Dot Public Schools California']
        indicators = indicators.loc[indicators['Time Off Date'] > CA_cal.iloc[0]['DATE_VALUE']]
        
    elif frame.equals(TX_output):
        indicators = indicators.loc[indicators['Company'] == 'Green Dot Public Schools Southeast Texas']
        indicators = indicators.loc[indicators['Time Off Date'] > TX_cal.iloc[0]['DATE_VALUE']]
    
    return(indicators)

WTO_CA = remove_WTO_during_LOA(CA_output)
WTO_TN = remove_WTO_during_LOA(TN_output)
WTO_TX = remove_WTO_during_LOA(TX_output)


# # # --------------------------------------------function to map new absence days total to the Workbooks-------------------

def map_new_absence_days(frame, WB_type):
    WTO_hours_dict = dict(frame.groupby(['Employee ID'])['Total Units'].sum())
    frame['Sum of Hours'] = frame['Employee ID'].map(WTO_hours_dict)
    WTO_absence = frame[['Employee ID', 'Sum of Hours']].drop_duplicates()
    WTO_absence['Total Days'] = WTO_absence['Sum of Hours'] / 8
    WTO_absence.loc[WTO_absence['Total Days'] < 0] = 0                # locate negative absence days, and replace with 0
    absence_dict = dict(zip(WTO_absence['Employee ID'], WTO_absence['Total Days']))
    WB_type['WTO Days'] = WB_type['Emp_ID'].map(absence_dict).fillna(0)
    return(WB_type)

WB_CA = map_new_absence_days(WTO_CA, WB_CA)
WB_TN = map_new_absence_days(WTO_TN, WB_TN)
WB_TX = map_new_absence_days(WTO_TX, WB_TX)



# ------------------------------------------workbook final calculations - -create Total Membership, Attendance Days / %-----------------------------------
#-------map School Common Name. 

def final_wbs(frame):
    frame['Calendar Start Date'] = frame['Calendar Start Date'].astype(float)
    frame['Calendar End Date'] = frame['Calendar End Date'].astype(float)
    frame['Total Membership'] = (frame['Calendar End Date'] - frame['Calendar Start Date']) + 1
    frame['Attendance Days'] = frame['Total Membership'] - frame['WTO Days'] - frame['LOA Days']
    frame['Attendance %'] = frame['Attendance Days'] / frame['Total Membership']
    frame['Attendance %'] = frame['Attendance %']* 100
    frame = frame.round(2)    
    return(frame)
    
    
WB_CA = final_wbs(WB_CA)   
WB_TN = final_wbs(WB_TN)
WB_TX = final_wbs(WB_TX)


# -----------------------------------------------------------------------------------------------------------------------
# Alter WTO_CA, WTO_TN, WTO_TX becuase subs have multiple termination dates, change their WTO to reflect new hire date
# Subs that are hired & rehired, their attendance will only reflect for their current stint,
# and their WTO from prior stints are removed as well as start date, term date. 

def fix_WTO(WB_state, frame):
    subs_old_WTO = list(WB_state['Emp_ID'].loc[WB_state['Attendance %'] < 0].values)
    subs_old_hire = list(WB_state['Hire_Date'].loc[WB_state['Attendance %'] < 0].values)

    for i in range(len(subs_old_WTO)):
        frame.drop(frame.loc[(WTO_['Employee ID'] == subs_old_WTO[0]) & (WTO_['Time Off Date'] <= subs_old_hire[0])].index, inplace = True)
        

fix_WTO(WB_CA, WTO_CA)
fix_WTO(WB_TN, WTO_TN)
fix_WTO(WB_TX, WTO_TX)

WB_CA = map_new_absence_days(WTO_CA, WB_CA)
WB_TN = map_new_absence_days(WTO_TN, WB_TN)
WB_TX = map_new_absence_days(WTO_TX, WB_TX)

WB_CA = final_wbs(WB_CA)   
WB_TN = final_wbs(WB_TN)
WB_TX = final_wbs(WB_TX)
# ----------------------------------------------------------------------------------------------------------------------

CA_common_name = {'Animo Venice Charter High School':'Venice',
                  'Animo Inglewood Charter High School':'Inglewood',
                  'Animo James B. Taylor Charter Middle School':'Animo James Taylor',
                  'Animo Ellen Ochoa Charter Middle School':'Animo Ellen Ochoa',
                  'Animo Legacy Charter Middle School':'Legacy',
                  'Animo Mae Jemison Charter Middle School':'Mae Jemison',
                  'Animo Jefferson Charter Middle School':'Jefferson',
                  'Animo Watts College Preparatory Academy':'Watts',
                  'Animo Ralph Bunche Charter High School':'Ralph Bunche',
                  'Animo Leadership Charter High School':'Leadership',
                  'Animo Compton Charter School':'Compton',
                  'Animo South Los Angeles Charter High School':'South LA',
                  'Oscar De La Hoya Animo Charter High School':'Oscar de la Hoya',
                  'Animo Jackie Robinson Charter High School':'Jackie Robinson',
                  'Alain Leroy Locke College Preparatory Academy':'Locke Academy',
                  'Animo Florence-Firestone Middle School':'Florence',
                  'Animo Pat Brown Charter High School':'Pat Brown',
                  'Animo City of Champions High School':'Champions'}

WB_CA['Location'] = WB_CA['Location'].map(CA_common_name)


TN_common_name = {'Wooddale Middle School':'Wooddale',
                  'Fairley High School':'Fairley',
                  'Bluff City High School':'Bluff City',
                  'Hillcrest High School':'Hillcrest',
                  'Kirby Middle School':'Kirby'}

WB_TN['Location'] = WB_TN['Location'].map(TN_common_name)

TX_common_name = {'M.L. King Middle School':'King Middle School'}
WB_TX['Location'] = WB_TX['Location'].map(TX_common_name)

# concat all frames into one. 
WB = pd.concat([WB_CA, WB_TN, WB_TX]).reset_index(drop = True)


# To get Region Performance.
# piv = WB.groupby(['Company'])[['Attendance Days', 'Total Membership']].sum()
# piv['Attendance %'] = piv['Attendance Days'] / piv['Total Membership']


# ------------------------------------------------------------------send to 89 server--------------------------------------

quoted = urllib.parse.quote_plus("Driver={SQL Server Native Client 11.0};"
                     "Server=10.0.0.89;"
                     "Database=DataTeamSandbox;"
                     "Trusted_Connection=yes;")

engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

# specify data types so it delivers in SQL correct. 
WB.to_sql('Workday_Employee_ADA', schema='dbo', con = engine, if_exists = 'replace', index = False,
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