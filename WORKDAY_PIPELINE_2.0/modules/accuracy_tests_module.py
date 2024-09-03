#Confirm each of the WBs come out with different WTO and LOA Days
import pandas as pd
import os


class accuracy_tests:

    csv_dir = os.path.join(os.getcwd(), 'csvs')

    @staticmethod
    def create_main_frame(acronym):

        WB_0 = pd.read_csv(accuracy_tests.csv_dir + f'\\{acronym}_WB_0.csv')[['Emp_ID', 'LOA Days', 'WTO Days']]
        WB_1 = pd.read_csv(accuracy_tests.csv_dir + f'\\{acronym}_WB_1.csv')[['Emp_ID', 'LOA Days', 'WTO Days']]
        WB_2 = pd.read_csv(accuracy_tests.csv_dir + f'\\{acronym}_WB_2.csv')[['Emp_ID', 'LOA Days', 'WTO Days']]

        merged_df = pd.merge(WB_0, WB_1, on='Emp_ID', suffixes=('_WB_2324', '_WB_2223'))
        merged_df = pd.merge(merged_df, WB_2, on='Emp_ID', suffixes=('', '_WB_2122'))

        return(merged_df)


    @staticmethod
    def accuracy_check_WTO(acronym, id_num):

   
        #Checking WTO Days 
        WTO_SY_0 = pd.read_csv(accuracy_tests.csv_dir + f'\\{acronym}_WTO_SY_0.csv')
        WTO_SY_1 = pd.read_csv(accuracy_tests.csv_dir + f'\\{acronym}_WTO_SY_1.csv')
        WTO_SY_2 = pd.read_csv(accuracy_tests.csv_dir + f'\\{acronym}_WTO_SY_2.csv')

        sub = WTO_SY_0.loc[WTO_SY_0['Employee ID'] == id_num]
        _2324 = sub.loc[(sub['Time Off Date'] >= '2023-08-14') & (sub['Time Off Date'] <= '2023-10-20')]['Total Units'].sum()/8

        sub = WTO_SY_1.loc[WTO_SY_1['Employee ID'] == id_num]
        _2223 = sub.loc[(sub['Time Off Date'] >= '2022-08-15') & (sub['Time Off Date'] <= '2023-06-16')]['Total Units'].sum()/8

        sub = WTO_SY_2.loc[WTO_SY_2['Employee ID'] == id_num]
        _2122 = sub.loc[(sub['Time Off Date'] >= '2021-08-11') & (sub['Time Off Date'] <= '2022-06-10')]['Total Units'].sum()/8


        print(f'2324 WTO Days {_2324}')
        print(f'2223 WTO Days {_2223}')
        print(f'2122 WTO Days {_2122}')

    @staticmethod
    def accuracy_check_LOA(acronym, id_num, region_first_day, region_last_day, LOA_iteration):

        #Checking LOA Days
        if LOA_iteration == 0:
            LOA_SY = pd.read_csv(accuracy_tests.csv_dir + f'\\{acronym}_LOA_SY_0.csv')
        elif LOA_iteration == 1:
            LOA_SY = pd.read_csv(accuracy_tests.csv_dir + f'\\{acronym}_LOA_SY_1.csv')
        elif LOA_iteration == 2:
            LOA_SY = pd.read_csv(accuracy_tests.csv_dir + f'\\{acronym}_LOA_SY_2.csv')


        region_first_day = region_first_day[2:4]
        region_last_day = region_last_day[2:4]

        sub = LOA_SY.loc[LOA_SY['Employee ID'] == id_num]
        sub['Total Leave Days'] = sub['Calendar End Date'] - sub['Calendar Start Date'] + 1

        sub = sub[
        #LOA begins before SY, but ends during 
        (((sub['First Day'] <= region_first_day) & (sub['Actual Last Day'] <= region_last_day)) |
        #LOA begins at the end of SY, and ends after the SY
        ((sub['First Day'] >= region_first_day) & (sub['First Day'] <= region_last_day)) |
        #LOA is within the SY
        ((sub['First Day'] >= region_first_day) & (sub['Actual Last Day'] <= region_last_day)))
        ]
            
        sub_total_leave = list(sub['Total Leave Days'].values)
            
        print(f'{region_first_day}{region_last_day} LOA Days {sub_total_leave}')

        #examples 
        # id_num = 1027

        # merged_df = accuracy_tests.create_main_frame('CA')
        # # display(merged_df)
        # accuracy_tests.accuracy_check_WTO('CA', id_num)
        # accuracy_tests.accuracy_check_LOA('CA', id_num, '2023-08-14', '2023-10-20', 0)
        # accuracy_tests.accuracy_check_LOA('CA', id_num, '2022-08-15', '2023-06-16', 1)
        # accuracy_tests.accuracy_check_LOA('CA', id_num, '2021-08-11', '2022-06-10', 2)


# final.groupby(['Company', 'School Year'])['Worker'].nunique()
#Final iteration