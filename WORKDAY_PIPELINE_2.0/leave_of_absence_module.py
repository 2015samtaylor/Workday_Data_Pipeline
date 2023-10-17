import pandas as pd
import logging
import numpy as np

class leave_of_absence:

    #map the first and last day for leaves of absences, also seperate LOA_ frame out in regions
    def map_LOA_first_last(LOA_, region_first_day, calendar_dict):
        
        #modifying first day
        LOA_.drop(LOA_.loc[LOA_['Actual Last Day'] < region_first_day].index, inplace = True)  # Drop all days where the Last Day of Leave is before the start of the SY. At that point they are meaningless
        LOA_['Calendar Start Date'] = LOA_['First Day'].map(calendar_dict).fillna('Error')   # With the remainders map the Calendar Last Day - See if First Day is on Calendar
        LOA_.loc[LOA_['First Day'] < region_first_day, 'Calendar Start Date'] = 1   # Now locate 'First Day' values that were before First Day, and give a Calendar Start Date of 1.

        #modifying actual last day
        LOA_['Calendar End Date'] = LOA_['Actual Last Day'].map(calendar_dict).fillna('Error') #map calendar end date based on actual last day, fillnas with errors
        LOA_['Actual Last Day'].replace({pd.NaT: 'Ongoing'}, inplace=True)                      #mark that pd.NaT as ongoing, because they are still on leave
        LOA_.loc[LOA_['Actual Last Day'] == 'Ongoing', 'Calendar End Date'] = len(calendar_dict) #replaces the values in the 'Calendar End Date' column for the rows where the 'Actual Last Day' is 'Ongoing' with the length (number of items) in the calendar_dict.
        LOA_['Actual Last Day'].replace({'Ongoing': pd.NaT}, inplace = True) #replace 'Ongoing' strings in Actual Last Day with pd.NaT values

        return(LOA_)


    def outliers_zip(output_list):

        #A PRIMARY KEY IS MADE HERE BECAUSE OF REPEAT LOA's UNDER ONE ID
        outliers = pd.DataFrame(output_list, columns = ['Emp_ID', 'TimeStamp', 'First Day'])
        outliers['PK'] = outliers['Emp_ID'] +  ' ' + outliers['First Day'].astype(str)
        outliers_zip = dict(zip(outliers['PK'], outliers['TimeStamp']))
        return(outliers_zip)


    def map_faulty_LOA(LOA_, sql_frame):

        #get all errors for Calendar End and Start Date
        e = LOA_.loc[(LOA_['Calendar End Date'] == 'Error') | (LOA_['Calendar Start Date'] == 'Error')]

        #utilize the calendar for a specific year
        df = sql_frame.set_index('DATE_VALUE')


        output_list_start = []
        output_list_end = []
        for index, row in e.iterrows():

            empid = row['Employee ID']
            start_date = row['Calendar Start Date']
            end_date = row['Calendar End Date']
            actual_last_day = row['Actual Last Day']
            #first day is present for primary key differentiation
            first_day = row['First Day']

            if start_date == 'Error':

                idx = df.index.get_loc(first_day, method='nearest')  #get nearest day based on index of df frame. 
                ts = df.index[idx]
                output = (empid,idx, first_day)
                output_list_start.append(output)

                
            if end_date == 'Error':

                idx = df.index.get_loc(actual_last_day, method='nearest')  #get nearest day based on index of df frame. 
                ts = df.index[idx]
                output = (empid,idx, first_day)
                output_list_end.append(output)

        #call on outliers_zip function to bring together lists, create primary key mapping, and 
        outliers_zip_start = leave_of_absence.outliers_zip(output_list_start)
        outliers_zip_end = leave_of_absence.outliers_zip(output_list_end)

        #outliers zip contains fixes for Calendar Start & End Date. 
        #map the proper First Day, and Actual Last Day for the employees that have hire dates on weekends, so it can match up with the calendar
        LOA_['PK'] = LOA_['Employee ID'] +  ' ' + LOA_['First Day'].astype(str)

        #map the outliers zip to their respective PKs
        LOA_['Calendar Start Date'] = LOA_['PK'].map(outliers_zip_start).fillna(LOA_['Calendar Start Date'])
        LOA_['Calendar End Date'] = LOA_['PK'].map(outliers_zip_end).fillna(LOA_['Calendar End Date'])

        #if the employee has a leave of absence in the system but not yet taken let there be a nan value for the calcs
        LOA_.loc[LOA_['Calendar Start Date'] > LOA_['Calendar End Date'], 'Calendar Start Date'] = np.nan

        return(LOA_)
    

    def create_total_leave_days(LOA_, region, sql_frame):

        
        fall_year = sql_frame.iloc[0]['DATE_VALUE'].year
        spring_year = sql_frame.iloc[-1]['DATE_VALUE'].year

        LOA_['Total Leave Days'] = LOA_['Calendar End Date'] - LOA_['Calendar Start Date'] + 1
        total_total = dict(LOA_.groupby(['Employee ID'])['Total Leave Days'].sum())

        region['LOA Days'] = region['Emp_ID'].map(total_total).fillna(0)
        region['School Year'] = str(fall_year) + '-' + str(spring_year)
        region = region[['School Year', 'Emp_ID', 'Company', 'Location', 'Worker', 'Title' ,'Hire_Date', 'Term_Date', 'Calendar Start Date', 'Calendar End Date', 'LOA Days']]

        #Only retain employees that were hired before the last day of the SY
        region = region.loc[region['Hire_Date'] < sql_frame.iloc[-1]['DATE_VALUE']]

        return(region)



    # # ----------------------------------loop through employees LOA, and WTO,identify where WTO was taken during LOA's and remove---
    # LOA frame will change with each script
    
    # def map_faulty_LOA(LOA_, sql_frame):

    def loop_through_emps(emp, LOA_, sql_frame, WTO_):
        output = []
            
        j = 0
        
        Emp_ID_list = list(LOA_['Employee ID'].unique())
        LOA_sub = LOA_.loc[LOA_['Employee ID'] == Emp_ID_list[emp]].reset_index(drop = True)
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
                    Last_Day_LOA = sql_frame.iloc[-1]['DATE_VALUE']

                vacation = (First_Day_LOA <= WTO_day) & (Last_Day_LOA >= WTO_day )

                out = [Emp_ID, First_Day_LOA, WTO_day, Last_Day_LOA, vacation]

                #conformed down to one output
                
                output.append(out)

                i += 1

            j += 1
        
        return(output)

    # # -------------------------------------calling on all employees to see if their Leave of Absences were during days off
    # IF the employee is currently on LOA the Last Day LOA will default to today. 

    def check(LOA_, sql_frame, WTO_):
        Emp_ID = list(LOA_['Employee ID'].unique())
        all_results = []  # Create an empty list to store results for all employees

        for i in range(len(Emp_ID)):
            emp_results = leave_of_absence.loop_through_emps(i, LOA_, sql_frame, WTO_)  # Call loop_through_emps for each employee
            all_results.extend(emp_results)  # Extend the all_results list with results for this employee

        out = pd.DataFrame(all_results, columns=['Employee ID', 'First Day LOA', 'WTO Day', 'Last Day LOA', 'Status'])
     

        return(out)
    
    # -----------------------------------------
    def remove_WTO_during_LOA(output, fall_cal):
        # identify days where time off days are during leave of Absence. Outliers variable holds Emp ID & WTO Day to remove
        during_LOA = output.loc[output['Status'] == True]
        during_LOA.reset_index(drop = True, inplace = True)
        outliers = pd.DataFrame(list(zip(during_LOA['Employee ID'], during_LOA['WTO Day'])), columns = ['Employee ID', 'WTO Day'])

        # outer join, and then filtering for left only to retain day WTO that was not during LOA. 
        indicators = pd.merge(WTO_, outliers, how = 'outer', left_on=['Employee ID', 'Time Off Date'], right_on = ['Employee ID', 'WTO Day'], indicator = True )
        indicators = indicators.loc[indicators['_merge'] == 'left_only']
        indicators.drop(columns = ['WTO Day', '_merge'],inplace = True)
        indicators.reset_index(drop = True, inplace = True)

        #This might need to be filtered down further
        indicators = indicators.loc[indicators['Time Off Date'] > fall_cal.iloc[0]['DATE_VALUE']]

        return(indicators)






