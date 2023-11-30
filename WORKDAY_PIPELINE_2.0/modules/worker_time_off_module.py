import pandas as pd
import logging

class worker_time_off:

    def remove_WTO_during_LOA(output, WTO_SY):

        # identify days where time off days are during leave of Absence. Outliers variable holds Emp ID & WTO Day to remove
        during_LOA = output.loc[output['Status'] == True]
        during_LOA.reset_index(drop = True, inplace = True)
        outliers = pd.DataFrame(list(zip(during_LOA['Employee ID'], during_LOA['WTO Day'])), columns = ['Employee ID', 'WTO Day'])

        # outer join, and then filtering for left only to retain day WTO that was not during LOA. 
        indicators = pd.merge(WTO_SY, outliers, how = 'outer', left_on=['Employee ID', 'Time Off Date'], right_on = ['Employee ID', 'WTO Day'], indicator = True )
        indicators = indicators.loc[indicators['_merge'] == 'left_only']
        indicators.drop(columns = ['WTO Day', '_merge'],inplace = True)
        indicators.reset_index(drop = True, inplace = True)

        first_day = indicators['Time Off Date'].min()
        last_day =  indicators['Time Off Date'].max()

        logging.info(f'WTO during the SY has been filtered during the time period of {first_day}-{last_day}')

        return(indicators)
    

    def map_new_absence_days(WTO_SY, WB):

        #Get total units of Worker TIme off and map by EMployee ID
        WTO_hours_dict = dict(WTO_SY.groupby(['Employee ID'])['Total Units'].sum())
        WTO_SY['Sum of Hours'] = WTO_SY['Employee ID'].map(WTO_hours_dict)

        #Take the necessities of WTO_absence, create the 'Total Days' of WTO and map to the WB
        WTO_absence = WTO_SY[['Employee ID', 'Sum of Hours']].drop_duplicates()
        WTO_absence['Total Days'] = WTO_absence['Sum of Hours'] / 8
        WTO_absence.loc[WTO_absence['Total Days'] < 0] = 0                # locate negative absence days, and replace with 0
        absence_dict = dict(zip(WTO_absence['Employee ID'], WTO_absence['Total Days']))
        WB['WTO Days'] = WB['Emp_ID'].map(absence_dict).fillna(0)
        return(WB)
    

    def final_wbs_modifications(WB):
        #if emp hired today
        WB = WB.loc[WB['Calendar Start Date'] != 'Error']

        WB['Calendar Start Date'] = WB['Calendar Start Date'].astype(float)
        WB['Calendar End Date'] = WB['Calendar End Date'].astype(float)
        WB['Total Membership'] = (WB['Calendar End Date'] - WB['Calendar Start Date']) + 1
        WB['Attendance Days'] = WB['Total Membership'] - WB['WTO Days'] - WB['LOA Days']
        WB['Attendance %'] = WB['Attendance Days'] / WB['Total Membership']
        WB['Attendance %'] = WB['Attendance %']* 100
        WB.loc[WB['Attendance %'] < 0, 'Attendance %'] = 0
        WB = WB.round(2)    
        return(WB)
    

    def mapping(region_acronym, WB):

        if region_acronym == 'CA':

            region_mapping = {'Animo Venice Charter High School':'Venice',
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
        
        elif region_acronym == 'TN':

            region_mapping = {'Wooddale Middle School':'Wooddale',
                        'Fairley High School':'Fairley',
                        'Bluff City High School':'Bluff City',
                        'Hillcrest High School':'Hillcrest',
                        'Kirby Middle School':'Kirby'}
            
        elif region_acronym == 'TX':

            region_mapping = {'M.L. King Middle School':'King Middle School'}


        WB['Location'] = WB['Location'].map(region_mapping)

        return(WB)

