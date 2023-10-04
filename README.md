### What is this repository for? ###

* Python script interacts with custom reports set up on Workday - Worker Time Off, Leave of Absence, All Roster, & Terminations in order to calculate Average Daily Attendance. 
* The major obstacle that was compensated in this process was Worker Time Off overlapping with Leaves of Absences. 
* The script is set to udpate once a week, given the custom reports from Workday update Monday mornings at 10:05 (Ran in a batch file). 

### How do I get set up? ###

* Must have proper credentials to access the custom reports. This is set up as the variables user, & password
* All necessary packages are listed at the top of the script. 
* Must be connected to the VPN to interact with the Data Warehouse. 

### Who do I talk to? ###

* Repo owner - Sam Taylor
* File will run on 10.0.0.124 server Monday mornings at 10:10 AM
