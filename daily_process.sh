#!/bin/bash
#Audience Launcher
source audience/bin/activate

python3 bs-campaigns-report.py all peru 
python3 bs-campaigns-report.py all colombia
python3 bs-campaigns-report.py all spain

