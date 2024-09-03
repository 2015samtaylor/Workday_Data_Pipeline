import pandas as pd
import os
# from modules.fixing_employee_calendar_module import fixing_employee_calendar
from modules.leave_of_absence_module import leave_of_absence
from modules.worker_time_off_module import worker_time_off

# -----------------------------------transform XML documents to Pandas DataFrames-----------------------------------

# def flatten_dict(d, parent_key='', sep='_'):
#     flattened = {}
#     for k, v in d.items():
#         # Create new key with parent key
#         new_key = f"{parent_key}{sep}{k}" if parent_key else k
        
#         if isinstance(v, dict):
#             # If the value is a dictionary, check if it has a '#text' key
#             if '#text' in v:
#                 flattened[new_key] = v['#text']
#             else:
#                 # Otherwise, recursively flatten the nested dictionary
#                 flattened.update(flatten_dict(v, new_key, sep))
#         else:
#             flattened[new_key] = v

#     # Simplify the keys by removing specific suffixes
#     simplified_flattened = {}
#     for k, v in flattened.items():
#         # Remove suffixes like '_wd:Instance' from the keys
#         if '_wd:Instance' in k:
#             new_key = k.split('_wd:Instance')[0]
#         else:
#             new_key = k
#         simplified_flattened[new_key] = v
    
#     return(simplified_flattened)




class transformation:

    def extract_fields(item):
        def extract_value(field, subfield=None):
            """ Helper function to extract values from potentially nested dictionaries. """
            value = item.get(field)
            if isinstance(value, dict):
                if subfield and isinstance(value.get(subfield), dict):
                    return value.get(subfield).get('#text')
                return value.get('@wd:Descriptor', value.get('#text'))
            return value
        
        return {
            'Report_Effective_Date': item.get('wd:Report_Date'),
            'Worker_Status': item.get('wd:Status'),
            'Emp_ID': item.get('wd:Emp_ID'),
            'Worker': extract_value('wd:Worker', 'wd:Instance'),
            'Last_Name': item.get('wd:Last_Name'),
            'First_Name': item.get('wd:First_Name'),
            'Company': item.get('wd:Company'),
            'Location': extract_value('wd:Location', 'wd:Instance'),
            'Business_Title': item.get('wd:Title'),
            'Employee_Type': extract_value('wd:Worker_Type', 'wd:Instance'),
            'Time_Type': extract_value('wd:Time_Type', 'wd:Instance'),
            'Hire_Date': item.get('wd:Hire_Date'),
            'Term_Date': item.get('wd:Term_Date'),
            'Original_Hire': item.get('wd:Original_Hire')
        }
    
    def get_All(All):
        # Apply the function to extract the fields from each dictionary
        flattened_data = [transformation.extract_fields(item) for item in All]

        # Convert the list of dictionaries into a DataFrame
        df = pd.DataFrame(flattened_data)

        return(df)
    
      
            
# ----------------------------------------------------iterate through JSON file, and send to Pandas Dataframe------------
    
    def get_LOA(LOA):
    
        total_emps = len(LOA)

        final = []

        i = 0
        while i < total_emps:

            emp_id = LOA[i]['wd:Worker_group']['wd:Emp_ID']
            company = LOA[i]['wd:Worker_group']['wd:Company']['wd:Instance']['#text']
            hire_date = LOA[i]['wd:Worker_group']['wd:Hire_Date']
            location = LOA[i]['wd:Worker_group']['wd:location']['wd:Instance']['#text']
            manager = LOA[i]['wd:Worker_group']['wd:Manager']['wd:Instance']['#text']
            emp_name = LOA[i]['wd:Worker']['wd:Instance']['#text']
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
            unit_of_time = LOA[i]['wd:Unit_of_Time']['wd:Instance']['#text']
            business_title = LOA[i]['wd:Worker_group']['wd:businessTitle']

            instance = [emp_id, emp_name, company, last_day, first_day, est_last_day, act_last_day, total_days, units_requested, unit_of_time, hire_date, location, manager, business_title ]
            final.append(instance)
            i += 1
        return(final)
    
#  -----------------------------------------------iterate through JSON file, and send to Pandas Dataframe------------ 
    
    def get_WTO(WTO):
    
        # get total employees
        total_emps = len(WTO)

        final = []
        # hang on while loop until gone through all emps
        i = 0
        while i < total_emps:
            one_emp = WTO[i]['wd:Time_Off_Completed_Details_group']
            emp_id = WTO[i]['wd:Employee_ID']
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

    def modify_all(All):
        All_ = transformation.get_All(All)
        # All_ = All_.rename(columns={'termination_date': 'Term_Date', 'Original_Hire_Date': 'Original_Hire', 'Employee_ID': 'Emp_ID'}) #YoY raw doc column change
        All_['Term_Date'] = All_['Term_Date'].str[0:10]
        All_['Hire_Date'] = All_['Hire_Date'].str[0:10]
        All_['Original_Hire'] = pd.to_datetime(All_['Original_Hire'])
        All_['Original_Hire'] =  All_['Original_Hire'].dt.date
         #All_ = All_.loc[All_['Title'].str.contains(''Teacher'')]
        All_.reset_index(drop = True, inplace = True)
        return(All_)


    def modify_LOA(LOA):
        final = transformation.get_LOA(LOA)
        LOA_ = pd.DataFrame(final, columns = ['Employee ID','Worker','Company','Last Day of Work','First Day','Estimated Last Day','Actual Last Day','Total Days','Units Requested','Unit of Time','Hire Date','Location','Manager','Business Title - Current'])
        # LOA_ = LOA_.loc[LOA_['Business Title - Current'].str.contains('Teacher')]
        LOA_.reset_index(drop = True, inplace = True)
        LOA_['First Day'] = LOA_['First Day'].str[0:10]
        LOA_['Actual Last Day'] = LOA_['Actual Last Day'].str[0:10]
        LOA_.reset_index(drop = True, inplace = True)
        return(LOA_)
    

    def modify_WTO(WTO):
        final = transformation.get_WTO(WTO)
        WTO_ = pd.DataFrame(final, columns = ['employee count','Worker', 'Worker Status','Employee ID',  'Hire Date', 'Termination Date', 'Job Profile', 'Company', 'Location', 'Time Off Date', 'Time Off Type for Time Off Entry', 'Total Units', 'Sum of Hours'])
        WTO_ = WTO_.drop_duplicates()
        WTO_['Total Units'] = WTO_['Total Units'].astype(float)
        #WTO_ = WTO_.loc[WTO_['Job Profile'].str.contains('Teacher')]
        WTO_.reset_index(drop = True, inplace = True)
        WTO_['Time Off Date'] = WTO_['Time Off Date'].str[0:10]
        return(WTO_)
    

    def singular_date_format(frame, col_name):

        frame[col_name] = pd.to_datetime(frame[col_name], format='%Y-%m-%d')

        date_list = []
        for dates in frame[col_name]:
            output = dates.replace(minute=0, second = 0, microsecond = 0)
            date_list.append(output)

        frame[col_name] = date_list

    def clear_up_initial_terminations(All_, region_first_day):

        initial = All_.loc[(All_['Term_Date'] > All_['Hire_Date']) & 
                        (All_['Term_Date'] > All_['Original_Hire'])&
                        (All_['Term_Date'] < region_first_day)]

        # Get the indexes from the 'initial' DataFrame
        indexes_to_drop = initial.index

        # Use drop to remove rows from the original DataFrame
        filtered_All_ = All_.drop(index=indexes_to_drop)

        return(filtered_All_)
    