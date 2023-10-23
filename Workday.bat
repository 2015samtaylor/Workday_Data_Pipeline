@echo off

cd C:\Users\samuel.taylor\Desktop\Python_Scripts\Workday\WORKDAY_PIPELINE_2.0

python.exe main.py >> .\logs\batch_file_log.txt 2>&1

echo Main.py has executed.

