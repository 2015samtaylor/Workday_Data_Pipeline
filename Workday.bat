@echo off
cd /d C:\Users\psadmin\Desktop\DW\Workday_ADA_Teachers\WORKDAY_PIPELINE_2.0
pipenv run python main.py >> .\logs\batch_file_log.txt 2>&1