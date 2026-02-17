#!/bin/bash

# Start Supervisor
/usr/bin/supervisord -n -c /etc/supervisor/supervisord.conf &

# Run the Python script
/home/second_careers_project/venv_secondcareers/bin/python3 /home/second_careers_project/schedular_2ndC.py
#/usr/bin/supervisord -n -c /etc/supervisor/supervisord_thread.conf &