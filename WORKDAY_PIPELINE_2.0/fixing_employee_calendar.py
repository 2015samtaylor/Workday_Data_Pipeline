import pandas as pd
from calendar_module import Calendar
import logging
from datetime import datetime
from transformation_module import data_transformation

#At the point where I can assume that I have the calendar for all regions

#First step is to seperate the all frame into regions

#Based on calendar start, & end date drop rows where emp has been terminated prior to the school year
# IF emp has not been terminated in the current School Year, then the End Date will default to most up to date. 
# If an EMP was fired today, it will not register on attendance until the next day. 

class specifics:

    def create_calendar_zip(calendar):

                #Create the calendar zip, if it is this SY have it filter for up to the current day
        calendar.reset_index(inplace = True)
        calendar['index'] = calendar['index'] + 1

        data_transformation.singular_date_format(calendar, 'DATE_VALUE')

        calendar = calendar[['index', 'DATE_VALUE']]
        calendar_dict = dict(zip(calendar['DATE_VALUE'], calendar['index']))

        return(calendar_dict)


    #Objective here is to dynamically get the last day of the calendar depending on the year
    def define_cal_end_date(calendar):

        spring, fall = Calendar.date_filter()

        #Get the year value of the last day of the calendar
        year_val = calendar.iloc[-1]['DATE_VALUE'].year


        if year_val < spring:

            print('Old Calendar')
            #If old calendar get the very last day of the SY to apply to the Calendar End Date
            calendar_end_date = calendar.iloc[-1]['DATE_VALUE']
            
            logging.info(f'Calendar End Date for Prior SY - {calendar_end_date}')

            calendar_dict = specifics.create_calendar_zip(calendar)

            #modify the calendar dict if we are currently in the SY. THis provides proper Calendar End Date
            calendar_dict = {key: value for key, value in calendar_dict.items() if key < region_last_day}
            
        else:
            #if the else block is triggered we are in the current SY, so grab the closest day to today
         
            # Get the current date and time
            current_datetime = datetime.now()
            ts = pd.to_datetime(current_datetime).date()  # Convert to date to remove the time component

            calendar = calendar.set_index('DATE_VALUE')
            iloc_idx = calendar.index.get_indexer([ts], method='nearest')
            # # Get the row with the nearest timestamp using the index
            nearest_row = calendar.iloc[iloc_idx]

            #pass in the timestamp to get the calendar end date value
            calendar_end_date = pd.Timestamp(nearest_row.index.values[0])

            logging.info(f'In the current SY - Calendar End Date - {calendar_end_date}')  

            calendar_dict = specifics.create_calendar_zip(calendar)      

        return(calendar_end_date, calendar_dict)



    def get_specifics(region_acronym, sql_calendar, complete_frame):

        spring, fall = Calendar.date_filter()

        if region_acronym == 'CA':
            full_name = 'Green Dot Public Schools California'
        elif region_acronym == 'TN':
            full_name = 'Green Dot Public Schools Tennessee'
        elif region_acronym == 'TX':
            full_name = 'Green Dot Public Schools Southeast Texas'
        else:
            print('Wrong region acronym')
            logging.info('Wrong region acronym as argument')



        #Potential solution. When querying for Last Day of School, always divert to the last day of the SQL calendar. 
        #Unless the spring year is greater than or equal to the year of the last day of School. 
        #In that case, use datetime today, and get the closest day. 


        #get the first day of August from the SQL pull. This is a catch if running in the summer for the upcoming SY
        try:
            region_first_day = sql_calendar.loc[sql_calendar['DATE_VALUE'].dt.month == 8].iloc[0]['DATE_VALUE']
            region_last_day, calendar_dict = specifics.define_cal_end_date(sql_calendar)

        except AttributeError:
            logging.info('First day of {}-{} School Year has not begun \n-----------------------'.format(fall.year, fall.year + 1))

        #Seperate into certain region
        region = complete_frame.loc[complete_frame['Company'] == full_name]


        return(region, calendar_dict, region_first_day, region_last_day)


#Region cal zip should be got outside of this 

#Use the SQL query to zip together the index and date value
# region_cal_zip = Calendar.calendar_zip(sql_calendar) 

