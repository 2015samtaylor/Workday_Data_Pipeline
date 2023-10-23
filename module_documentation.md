# This module documentation breaks down each module as they occur in the script in linear fashion

# date_request_module summary

1. The class `Data_Request` is initialized with the user and password required for authentication to access Workday reports.

2. The `get_report` method is defined to obtain a report from a given URL. It performs the following steps:

   - It creates a session using the `requests` library and sets up HTTP basic authentication with the provided user and password.
   - It sends an HTTP POST request to the specified URL to authenticate.
   - It sends an HTTP GET request to the same URL to retrieve the report data.

3. If the response status code is 200 (indicating a successful request), the method does the following:

   - Logs the successful request with the URL and status code.
   - Parses the XML response content using the `xmltodict` library, converting it into a JSON-formatted string.
   - Loads the JSON data into a Python dictionary, specifically extracting the 'wd:Report_Data' and 'wd:Report_Entry' parts of the response.
   - Returns the report data as a dictionary.

4. If the response status code is not 200, it raises an exception and logs a failure message.

In summary, this code provides a Python class that can be used to authenticate and retrieve Workday reports by sending authenticated requests to a provided URL. The retrieved report data is processed and returned as a Python dictionary. This class can be utilized for integrating Workday report retrieval into other applications or scripts.

--------------------------------------------

# transformation_module summary

1. **`transform(doc)` Method:**
   - This method is designed to transform a list of XML documents into Pandas DataFrames.
   - It takes a list of XML documents (`doc`) as input.
   - It iterates through the list, creating a Pandas DataFrame for each XML document.
   - The method then resets the index of the DataFrame, removes a specific column ('1'), and appends the DataFrame to the `output` list.
   - All the DataFrames in the `output` list are concatenated into a single Pandas DataFrame.
   - It also performs some modifications on the column names, removing 'WD' from the column names.
   - The resulting Pandas DataFrame is returned.

2. **`get_LOA(LOA)` Method:**
   - This method extracts specific information from a list of JSON objects representing employee leave of absence (LOA) data.
   - It takes a list of LOA JSON objects (`LOA`) as input.
   - The method iterates through the list of LOA records, extracting information such as employee ID, company, hire date, location, and more.
   - The extracted information is structured into a list of instances, with each instance representing an employee's LOA details.
   - The list of instances is returned as the output.

3. **`get_WTO(WTO)` Method:**
   - This method is similar to `get_LOA` but specifically designed for JSON objects representing employee work-time off (WTO) data.
   - It extracts information like employee ID, worker status, job profile, and time off details.
   - The extracted information is organized into a list of instances and returned as the output.

4. **`modify_all(All)` Method:**
   - This method is used to modify and format a DataFrame containing various data (`All`).
   - It makes several modifications to the DataFrame, such as formatting date columns, removing 'Teacher' rows, and resetting the index.
   - The modified DataFrame is returned.

5. **`modify_LOA(LOA)` Method:**
   - This method is specifically for modifying and formatting a DataFrame containing LOA data (`LOA`).
   - It formats date columns, removes rows containing 'Teacher' in the 'Business Title - Current' column, and resets the index.
   - The modified DataFrame is returned.

6. **`modify_WTO(WTO)` Method:**
   - Similar to `modify_LOA`, this method is used for formatting a DataFrame with WTO data (`WTO`).
   - It removes duplicates, converts the 'Total Units' column to a float data type, and formats the date column.
   - The modified DataFrame is returned.

7. **`singular_date_format(frame, col_name)` Method:**
   - This method is a utility function for formatting date columns in a Pandas DataFrame.
   - It converts the specified date column to a consistent format, removing minutes, seconds, and microseconds.

In summary, this Python module provides a set of methods for transforming and modifying XML and JSON data into structured Pandas DataFrames, making the data more accessible and ready for analysis.

---------------------------------------------

# calendar_query_module

1. **`calendar_query` Class:**
   - This class is responsible for querying a SQL database for calendar data, applying date filters, and returning the result as a Pandas DataFrame.
   - It contains three main methods to interact with different SQL servers and retrieve specific calendar data based on the region's acronym.

2. **`SQL_query` Method:**
   - This method performs an SQL query to retrieve calendar data from a specified SQL server (GD_DW).
   - It establishes a database connection using the `pyodbc` library, retrieves data using the provided query, and logs the status of the SQL call.
   - The retrieved data is returned as a Pandas DataFrame.

3. **`SQL_query_TX` Method:**
   - Similar to `SQL_query`, this method is used to query a different SQL server (GD_DW_94) and retrieve region-specific calendar data for Texas (TX).

4. **`date_filter` Method:**
   - This utility method calculates the start and end dates for the spring and fall academic years based on the current date.
   - It returns the calculated spring and fall years.

5. **`calendar_process` Method:**
   - This method establishes the region's calendar data by making SQL queries based on the region's acronym and academic year.
   - Depending on the region (`region_acronym`), it constructs a region-specific SQL query to fetch calendar data for that region.
   - The retrieved calendar data is returned as a Pandas DataFrame.

In summary, this Python module focuses on querying a SQL database to fetch region-specific calendar data. It provides methods to retrieve this data for different regions and academic years. The data can then be used for various calendar-related tasks and analyses.

---------------------------------------------

# fixing_employee_calendar_module

1. **`create_calendar_zip` Method:**
   - This method takes a calendar DataFrame and prepares it for easy access.
   - It increments the calendar index and formats date values for future reference.
   - It returns a dictionary where dates are mapped to their corresponding index in the calendar.

2. **`define_cal_end_date` Method:**
   - This method determines the end date of the calendar, which varies based on the academic year.
   - It identifies whether the current date falls within the academic year (SY).
   - If the SY is current, it retrieves the nearest date to the current date, considering today's date. Otherwise, it takes the last day of the calendar.
   - It returns the calendar end date and the modified calendar dictionary.

3. **`get_specifics` Method:**
   - This method gets specific details based on the region's calendar and the academic year (fall and spring) and handles region-specific logic.
   - It identifies the first and last day of school for the region and returns the region's data along with the calendar dictionary and academic year details.

4. **`insert_starts_ends` Method:**
   - This method inserts start and end dates into the region's data.
   - It maps the calendar start dates based on the hire date, considering whether the hire date is before the first day of school.
   - It also handles employee terminations, setting a proper calendar end date based on the termination date.
   - Employees terminated before the first day of school are removed from the dataset.
   - It returns the region's data with updated calendar start and end dates.

5. **`calendar_errors` Method:**
   - This method handles calendar errors by mapping the proper hire and termination dates based on calendar start or end date issues.
   - It locates employees with "Error" in their calendar start or end dates and identifies their nearest working day in the calendar.
   - It maps these dates and updates the corresponding calendar start or end dates.
   - Special cases are handled, such as employees hired or terminated on the current date, which are given temporary values.
   - It returns the region's data with corrected calendar start and end dates.

In summary, this module focuses on adjusting and correcting employee calendar data by ensuring that calendar start and end dates align with academic years and handle various exceptional cases. The module is particularly useful for preparing employee data for analysis based on academic calendars.

---------------------------------------------

# leave_of_absence_module

1. **`fix_calendar_start_end_date` Method:**
   - This method aims to correct calendar start and end dates in LOA data.
   - It identifies rows with "Error" in the calendar start or end date and finds the nearest working calendar day based on a provided calendar dictionary.
   - It then maps these corrected values to the LOA data, updating calendar start and end dates.
   - It is a helper method used for adjusting LOA data based on calendar start and end dates.

2. **`map_LOA_first_last` Method:**
   - This method takes LOA data, a calendar dictionary, and academic year start and end dates as input.
   - It filters LOA data to retain records where the LOA period intersects with the academic year.
   - It calculates calendar start and end dates based on LOA start and end dates and the provided calendar dictionary.
   - Special cases are handled, such as LOA periods starting before the academic year or ending after.
   - It updates calendar start and end dates in the LOA data and handles exceptional cases.
   - It is used to ensure that LOA data aligns with academic calendar dates.

3. **`create_total_leave_days` Method:**
   - This method takes LOA data, region data, and SQL calendar data as input.
   - It calculates the total leave days for each employee and maps this information to the region data.
   - The academic year is determined based on the provided SQL calendar data.
   - It is used to determine the total leave days for employees based on LOA data and maps this information to the region data.

4. **`loop_through_emps` Method:**
   - This method is used to iterate through employees in LOA data and check if their LOA dates overlap with days off (WTO).
   - It takes an employee index, LOA data, SQL calendar data, and WTO data as input.
   - It checks for overlaps between LOA periods and days off (WTO) for the specified employee.
   - It returns a list of information regarding these overlaps.

5. **`check` Method:**
   - This method checks if LOA dates for all employees overlap with days off (WTO) and returns the results.
   - It filters the WTO data to include only days within the specified academic year.
   - It then iterates through all employees to check if their LOA dates overlap with days off using the `loop_through_emps` method.
   - The results are returned in a DataFrame, including employee ID, LOA start and end dates, WTO day, and status.

In summary, this class is designed to process LOA data and ensure that LOA periods are correctly aligned with academic calendar dates, and it also checks for overlaps between LOA and days off (WTO). The methods provided can be used to clean and adjust LOA data to make it suitable for further analysis and reporting.

---------------------------------------------

# worker_time_off_module

1. **`remove_WTO_during_LOA` Method:**
   - This method takes an output DataFrame containing information about overlapping WTO during LOA and the original WTO data (WTO_SY).
   - It identifies days where WTO occurred during LOA and removes these days from the original WTO data.
   - An outer join is performed to identify which WTO days were during LOA, and the "left_only" rows are retained.
   - The earliest and latest WTO dates are logged, and the modified WTO data is returned.

2. **`map_new_absence_days` Method:**
   - This method takes WTO data (WTO_SY) and worker balance (WB) data as input.
   - It calculates the total units of WTO hours for each employee and maps them to the WB data.
   - Total WTO days are calculated based on WTO hours, and negative absence days are replaced with zero.
   - The total WTO days are mapped to the WB data, and the updated WB data is returned.

3. **`final_wbs_modifications` Method:**
   - This method takes worker balance (WB) data as input.
   - It performs several calculations on the WB data, including calculating the total membership, attendance days, and attendance percentage.
   - Negative attendance percentages are replaced with zero.
   - The WB data is rounded to two decimal places, and the modified WB data is returned.

4. **`mapping` Method:**
   - This method takes a region acronym and worker balance (WB) data as input.
   - It maps the locations in the WB data to their corresponding region names based on the provided region acronym.
   - The updated WB data with mapped locations is returned.

In summary, this class is designed to process WTO data and worker balance (WB) data, remove WTO days during LOA, calculate and map total WTO days to worker balances, and perform final modifications on worker balance data, including mapping locations to their corresponding region names. These methods are used to prepare and clean the worker balance data for analysis and reporting.

-----------------------------------------
# process

Finally, the process function is called in a for loop, a total of three times (2021-2022, 2022-2023, 2023-2024) for each region ('CA', 'TX', 'TN'), and the results are stored in the CA, TX, and TN variables.

-------------------------------------------
# accuracy

1. **`create_main_frame(acronym)`:**
   - This method reads the processed worker balance (WB) data from CSV files for three different calendar years (indicated by suffixes '_WB_2324', '_WB_2223', and '_WB_2122'). It merges the data frames and returns a merged DataFrame.

2. **`accuracy_check_WTO(acronym, id_num)`:**
   - This method checks the accuracy of WTO (worker time off) days for a specific employee (`id_num`) across different calendar years.
   - It reads the processed WTO data from CSV files for each calendar year ('_WTO_SY_0', '_WTO_SY_1', '_WTO_SY_2').
   - It calculates and prints the total WTO days for the employee in each calendar year.

3. **`accuracy_check_LOA(acronym, id_num, region_first_day, region_last_day, LOA_iteration)`:**
   - This method checks the accuracy of LOA (leave of absence) days for a specific employee (`id_num`) across different calendar years.
   - It reads the processed LOA data from CSV files for each calendar year ('_LOA_SY_0', '_LOA_SY_1', '_LOA_SY_2').
   - It calculates and prints the total LOA days for the employee in each calendar year.
   - The `region_first_day` and `region_last_day` parameters specify the date range for the region's academic year.
   - The `LOA_iteration` parameter indicates the calendar year iteration (0, 1, or 2) to retrieve the corresponding LOA data.

4. **Examples:**
   - The examples provided at the end of the class show how to use these accuracy check methods. They demonstrate how to create a main DataFrame for a specific region, check the accuracy of WTO days, and check the accuracy of LOA days for an employee in different calendar years.

These methods are designed to verify the accuracy of processed data by comparing the calculated WTO and LOA days for a specific employee across multiple academic years. This can help ensure that the data processing and modifications are correct and consistent over time.