import pandas as pd
import pyodbc
import logging
import datetime
from transformation_module import data_transformation


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
        data_transformation.singular_date_format(most_recent, 'DATE_VALUE')
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


    def calendar_process(region_acronym, fall, spring, complete_frame):
     
        #make an instance of the Calendar Class and establish the regions calendar
        
        if region_acronym  == 'CA':
  
            CA = Calendar('''
            SELECT [SCHOOLID] ,[DATE_VALUE] ,[MEMBERSHIPVALUE] 
            FROM [PowerschoolStaged].[dbo].[CALENDAR_DAY] 
            where DATE_VALUE >'{}-08-01' and DATE_VALUE <= '{}-07-01'
            and SCHOOLID=124016 and MEMBERSHIPVALUE=1  order by DATE_VALUE
            '''.format(fall, spring))
            region_cal = CA.SQL_query()


        elif region_acronym == 'TN':

            TN = Calendar('''
            SELECT [SCHOOLID] ,[DATE_VALUE] ,[MEMBERSHIPVALUE] 
            FROM [PowerschoolStaged].[dbo].[CALENDAR_DAY] 
            where DATE_VALUE >'{}-08-01' and DATE_VALUE <= '{}-07-01'
            and SCHOOLID=8055 and MEMBERSHIPVALUE=1  order by DATE_VALUE
            '''.format(fall, spring))

            region_cal = TN.SQL_query()

        elif region_acronym == 'TX':

            TX=Calendar('''
            SELECT DISTINCT [STUDENT_CAL_DATE] AS DATE_VALUE, [STUDENT_CAL_DATE_TYPE], [STUDENT_CAL_FISCAL_YEAR] 
            FROM [Frontline].[dbo].[STUDENT_CALENDAR_DATE] 
            WHERE STUDENT_CAL_DATE_TYPE = 'Instructional'
            AND STUDENT_CAL_DATE > '{}-08-01'
            AND STUDENT_CAL_DATE <= '{}-07-01' ORDER BY STUDENT_CAL_DATE
            '''.format(fall, spring))

            region_cal = TX.SQL_query_TX()

        return(region_cal)
    

#probably going to need to move full_name to fixing_start_ends_module










