#!/bin/bash
#Audience Launcher
source audience/bin/activate

python3 bs-campaigns-report.py all PERU
python3 bs-campaigns-report.py all COLOMBIA
python3 bs-campaigns-report.py all SPAIN

