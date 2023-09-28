import pandas as pd

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
    
    def get_LOA(LOA):
    
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
    
    def get_WTO(WTO):
    
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

    def modify_all(All):
        All_ = data_transformation.transform(All)
        All_['Term_Date'] = All_['Term_Date'].str[0:10]
        All_['Hire_Date'] = All_['Hire_Date'].str[0:10]
         #All_ = All_.loc[All_['Title'].str.contains(''Teacher'')]
        All_.reset_index(drop = True, inplace = True)
        return(All_)


    def modify_LOA(LOA):
        final = data_transformation.get_LOA(LOA)
        LOA_ = pd.DataFrame(final, columns = ['Employee ID','Worker','Company','Last Day of Work','First Day','Estimated Last Day','Actual Last Day','Total Days','Units Requested','Unit of Time','Hire Date','Location','Manager','Business Title - Current'])
        # LOA_ = LOA_.loc[LOA_['Business Title - Current'].str.contains('Teacher')]
        LOA_.reset_index(drop = True, inplace = True)
        LOA_['First Day'] = LOA_['First Day'].str[0:10]
        LOA_['Actual Last Day'] = LOA_['Actual Last Day'].str[0:10]
        LOA_.reset_index(drop = True, inplace = True)
        return(LOA_)
    

    def modify_WTO(WTO):
        final = data_transformation.get_WTO(WTO)
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
    