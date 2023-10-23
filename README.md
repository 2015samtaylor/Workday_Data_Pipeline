### What is this repository for? ###

* Python script interacts with custom reports set up on Workday - Worker Time Off, Leave of Absence, All Roster, & Terminations in order to calculate Average Daily Attendance.
* The script is set to udpate once a week, given the custom reports from Workday update Monday mornings at 10:05 (Ran in a batch file). 


### Major pipeline obstacles ###
* No API access, and using a workaround by making a request to Workday files and parsing them coming across as XML files
* Utilizing multiple Calendars for multiple regions for multiple years
* Accounting for leaves of absences beginning before the school year and ending during
* Accounting for leaves of absences beginning during the school year, and ending after
* Worker Time Off overlapping with Leaves of Absences

### How do I get set up? ###

* Must have proper credentials to access the custom reports. This is set up as the variables user, & password 
* Must be connected to the VPN to interact with the Data Warehouse. 

## Prerequisites

- [Python](https://www.python.org/downloads/) (3.x)
- [Pipenv](https://pipenv.pypa.io/en/latest/)

## Getting Started

To get started with this project, follow the steps below:

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/yourusername/your-repo.git
   cd your-repo
   ```  
   Modify the config.py to have proper credentials to make requests to the Workday reports

2. **Install Pipenv:**

   If you haven't already installed Pipenv, you can do so using pip (Python's package manager):

   ```bash
   pip install pipenv
   ```

3. **Set Up the Virtual Environment:**

   Navigate to your project directory and create a virtual environment using Pipenv:

   ```bash
   pipenv shell
   ```

   This command will create and activate a virtual environment specific to your project.

4. **Install Project Dependencies:**

   Inside the virtual environment, use the `pipenv sync` command to install the project's dependencies:

   ```bash
   pipenv sync
   ```

   This command reads dependencies from the `Pipfile` and ensures the virtual environment is in sync.

5. **Run the Main Script:**

   With the virtual environment active and dependencies installed, you can run your main Python script 

   ```bash
   python main.py
   ```

### Who do I talk to? ###

* Repo owner - Sam Taylor
* File will run on 10.0.0.124 server Monday mornings at 10:10 AM


