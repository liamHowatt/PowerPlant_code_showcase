# PowerPlant Code Showcase
Showcase of services and other scripts used in PowerPlant.

This code in this repo is here for reading.

> **While you're here, check out my other interesting repositories**
> - [Gameshell Multimeter](https://github.com/liamHowatt/Gameshell-Multimeter) - a 3rd party game console transformed into a multimeter
> - [Osepp_PROX_01](https://github.com/liamHowatt/Osepp_PROX_01) - an Arduino library for an off-the-shelf sensor that otherwise has no documentation
> - [lite_vec](https://github.com/liamHowatt/lite_vec) - Minimal growable vector/buffer in C

## Explainations

Modbus to MQTT gateway (Python). Named this way because it replaced the functionality of Node Red  
`pyred.py`  

Modbus to MQTT gateway (C implementation of `pyred.py` - can only read Modbus and not write)  
`cred.c`  
    Experimental multithreded versions of `cred.c` to read Modbus and publish to MQTT concurrently.  
    `cred_mt_v1.c`  
    `cred_mt_v2.c`  

PLC tag value logger  
`logger.py`

HTTP backend server to support a frontend web user interface  
`api.py`

A service that controls a PLC to follow a programmed recipe  
`recipe_update_v3.py`

Capture and store images from IP cameras  
`vision.py`

Report faults raised by PLC  
`fault_reporter.py`

Hacky way of generating `.ccwmod` files. XML files that map PLC tags to Modbus registers  
`converter.py`

Send the current time to PLCs  
`time_sync.py`

Small Streamlit app for updating the database and the Modbus - MQTT gateway with new tags  
`tag_gui.py`

Turns a Raspberry Pi into an MQTT connected network camera  
`picam_main.c`

Connect to a Raspberry Pi running `picam_main.c` to view live frames  
`picam_disp.py`

Similar to `picam_disp.py`, but connects to two Pis instead of one  
`picam_disp_2.py`