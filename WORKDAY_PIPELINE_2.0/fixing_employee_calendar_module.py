import pandas as pd
from calendar_query_module import calendar_query
import logging
from datetime import datetime
from transformation_module import transformation

#At the point where I can assume that I have the calendar for all regions

#First step is to seperate the all frame into regions

#Based on calendar start, & end date drop rows where emp has been terminated prior to the school year
# IF emp has not been terminated in the current School Year, then the End Date will default to most up to date. 
# If an EMP was fired today, it will not register on attendance until the next day. 

class fixing_employee_calendar:

    def create_calendar_zip(calendar):

        #Create the calendar zip, if it is this SY have it filter for up to the current day
        calendar.reset_index(inplace = True)
        calendar['index'] = calendar['index'] + 1

        transformation.singular_date_format(calendar, 'DATE_VALUE')

        calendar = calendar[['index', 'DATE_VALUE']]
        calendar_dict = dict(zip(calendar['DATE_VALUE'], calendar['index']))

        return(calendar_dict)


    #Objective here is to dynamically get the last day of the calendar depending on the year
    def define_cal_end_date(calendar):

        spring, fall = calendar_query.date_filter()

        #Get the year value of the last day of the calendar
        year_val = calendar.iloc[-1]['DATE_VALUE'].year


        if year_val < spring:

            print('Old Calendar')
            #If old calendar get the very last day of the SY to apply to the Calendar End Date
            calendar_end_date = calendar.iloc[-1]['DATE_VALUE']
            
            logging.info(f'Calendar End Date for Prior SY - {calendar_end_date}')

            calendar_dict = fixing_employee_calendar.create_calendar_zip(calendar)
            
        else:
            #if the else block is triggered we are in the current SY, so grab the closest day to today

            #create the calendar dict instance to be modified. 
            calendar_dict = fixing_employee_calendar.create_calendar_zip(calendar)

            # Get the current date and time
            current_datetime = datetime.now()
            ts = pd.to_datetime(current_datetime).date()  # Convert to date to remove the time component

            calendar = calendar.set_index('DATE_VALUE')
            iloc_idx = calendar.index.get_indexer([ts], method='nearest')
            # # Get the row with the nearest timestamp using the index
            nearest_row = calendar.iloc[iloc_idx]

            #pass in the timestamp to get the calendar end date value
            calendar_end_date = pd.Timestamp(nearest_row.index.values[0])

            #modify the calendar dict if we are currently in the SY. THis provides proper Calendar End Date
            calendar_dict = {key: value for key, value in calendar_dict.items() if key < calendar_end_date}

            print(f'In the current SY = Calendar End Date = {calendar_end_date}')
            logging.info(f'In the current SY - Calendar End Date - {calendar_end_date}')     

        return(calendar_end_date, calendar_dict)



    def get_specifics(sql_calendar, complete_frame):

        spring, fall = calendar_query.date_filter()

        #This is here to simply declare the All_ as region variable
        region = complete_frame

        #When querying for Last Day of School, it always diverts to the last day of the SQL calendar. 
        #Unless the spring year is greater than or equal to the year of the last day of School. 
        #In that case, use datetime today, and get the closest day. 

        try:
            region_first_day = sql_calendar.loc[sql_calendar['DATE_VALUE'].dt.month == 8].iloc[0]['DATE_VALUE']
            region_last_day, calendar_dict = fixing_employee_calendar.define_cal_end_date(sql_calendar)

        except AttributeError:
            logging.info(f'First day of {fall.year}-{fall.year+1} School Year has not begun \n-----------------------')


        return(region, calendar_dict, region_first_day, region_last_day)
    

    def insert_starts_ends(region, calendar_dict, region_first_day):

        #Map the Calendar Start Dates based on the Hire Date. If the Hire Date is prior to the first day of school
        #default to a value of 1. 
        region['Calendar Start Date'] = region['Hire_Date'].map(calendar_dict).fillna('Error')
        region.loc[region['Hire_Date'] < region_first_day, 'Calendar Start Date'] = 1

        #If the Employed is present on the second loop, restore the pd.NaT type and DateTime of the Term Date column
        region['Term_Date'].replace({'Employed': pd.NaT}, inplace=True)
        region['Term_Date'] = pd.to_datetime(region['Term_Date'])

        #If termination date is before the first day, then drop the employee from this particular frame
        #Create a Calendar End Date column that is based on the Term_Date
        #If the Term Date is on the Calendar for this year then it receives a date value from the zip frame. 
        #Else it will locate values from 'Term_Date' column that says Employed and span it out the be the length of the zip frame

        region.drop(region.loc[region['Term_Date'] < region_first_day].index, inplace = True)
        region['Term_Date'].replace({pd.NaT: 'Employed'}, inplace=True)
        region['Calendar End Date'] = region['Term_Date'].map(calendar_dict).fillna('Error')
        region.loc[region['Term_Date'] == 'Employed', 'Calendar End Date'] = len(calendar_dict)


    #Map the proper Hire Date and Term Date, which inherently influences Calendar Start End Dates.

    def calendar_errors(region, sql_frame, calendar_dict, calendar_start_or_end):

        if calendar_start_or_end == 'Calendar Start Date':
            e = region.loc[(region['Calendar Start Date'] == 'Error')]

        elif calendar_start_or_end == 'Calendar End Date':
            e = region.loc[(region['Calendar End Date'] == 'Error')]

        else:
            print("Wrong column for Calendar Start or End")
        
        df = sql_frame.set_index('DATE_VALUE')

        output_list = []
    
        for index, row in e.iterrows():

            empid = row['Emp_ID']
            company = row['Location']


            if row[calendar_start_or_end] == 'Error':

                # Locate Hire_Date
                faulty_date = row['Hire_Date']
                logging.info(f"Emp_ID: {row['Emp_ID']} - Error in Calendar Start Date, Hire_Date: {faulty_date} Location: {company}")

                idx = df.index.get_loc(faulty_date, method='nearest')  #get nearest Calendar day based on index of df frame. 
                ts = df.index[idx]

                output = (empid, ts)    #get emp_id and timestamps, turn into df
                output_list.append(output)



        outliers_zip = pd.DataFrame(output_list, columns = ['Emp_ID', 'TimeStamp'])
        outliers_zip = dict(zip(outliers_zip['Emp_ID'], outliers_zip['TimeStamp']))

        # map the proper Hire Date for the employees that have hire dates on weekends, put in closest working day prior

        if calendar_start_or_end == 'Calendar Start Date':
            region['Hire_Date'] = region['Emp_ID'].map(outliers_zip).fillna(region['Hire_Date'])
            #Now that new Hire Dates are mapped, re-map Calendar Start Dates
            region['Calendar Start Date'] = region['Hire_Date'].map(calendar_dict).fillna(region['Calendar Start Date'])

        elif calendar_start_or_end == 'Calendar End Date':
            region['Term_Date'] = region['Emp_ID'].map(outliers_zip).fillna(region['Term_Date'])
            #Now that new Term Dates are mapped, re-map Calendar End Dates
            region['Calendar End Date'] = region['Term_Date'].map(calendar_dict).fillna(region['Calendar End Date'])


        ####Unique Cases for Filtering#####

        # #If an 'Error' still persists, that means they got hired today. In that case give them a temp Calendar Start Date of yesterday.
        region.loc[region['Calendar Start Date'] == 'Error', 'Calendar Start Date'] = calendar_dict[max(calendar_dict.keys())]

        # #Same thing with the terminations, if an error is persisting with the Calendar End Date means they got terminated today. Give them a temp last day of most up to date
        region.loc[region['Calendar End Date'] == 'Error', 'Calendar End Date'] = calendar_dict[max(calendar_dict.keys())]

        # Catch instances where Subs are rehired after term dates, and give new Calendar End Date. 
        region.loc[region['Calendar Start Date'] > region['Calendar End Date'], 'Calendar End Date'] = calendar_dict[max(calendar_dict.keys())]

        return(region)

