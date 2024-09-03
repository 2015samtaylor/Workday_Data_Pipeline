
import os
from modules.fixing_employee_calendar_module import *
from modules.transformation_module import *


def process(WTO_, LOA_, All_, fall_cal, year_str):

    #Filter all of the frames in the operation to their respected region  
    filt = 'Green Dot Public Schools California'
    
    WTO_ = WTO_.loc[WTO_['Company'] == filt].reset_index(drop = True)
    LOA_ = LOA_.loc[LOA_['Company'] == filt].reset_index(drop = True)
    All_ = All_.loc[All_['Company'] == filt].reset_index(drop = True)

    # Create the directory if it doesn't exist for accuracy checks on csvs
    csv_dir = os.path.join(os.getcwd(), 'csvs')
    os.makedirs(csv_dir, exist_ok=True)

    # # #get specific calendar dict, region, first_day, and last_day of a fall calendar year
    # This is restarted at every iteration of the for loop because it stems from the All_ frame, and is filtered with fall_cal
    calendar_dict, region_first_day, region_last_day = fixing_employee_calendar.get_specifics(fall_cal)

    #Testing to drop emps that have terminations prior to the start of the SY & are not re-hired
    region = transformation.clear_up_initial_terminations(All_, region_first_day)

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
    fixing_employee_calendar.write_out_terminations(WB, region_original, year_str)

    #create an output that tells whether WTO was during LOA
    output, WTO_SY = leave_of_absence.check(LOA_SY, fall_cal, WTO_, region_first_day, region_last_day)

    #Return the WTO for this SY, with wrong days removed
    WTO_SY = worker_time_off.remove_WTO_during_LOA(output, WTO_SY)

    WB = worker_time_off.map_new_absence_days(WTO_SY, WB)
    

    #Identify instances when rehired in middle of year after a termination
    rehires = fixing_employee_calendar.locate_rehires(WB, region_first_day, region_last_day)

    #Change the Calendar Dates for rehires, and map back to the WB
    WB = fixing_employee_calendar.fix_rehire_calendar_dates(rehires, fall_cal, calendar_dict, WB)
    WB = fixing_employee_calendar.fix_dates_out_of_calendar(fall_cal, calendar_dict, WB, region_first_day, region_last_day, 'Term_Date')
    
    WB = worker_time_off.final_wbs_modifications(WB)   
    WB = worker_time_off.mapping(WB)
    
    return(WB)