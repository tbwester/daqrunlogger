# daqrunlogger

Collection of short classes to log DAQ information to different platforms.

Planned platforms:
- Google sheets
- Fermilab ECL (electronic logbook). Requires the ![sbndprmdaq](https://github.com/marcodeltutto/SBNDPurityMonitorDAQ) package.
- Post JSON string of the current run to a server for other APIs

Additionally, we provide a daemon class to handle executing the loggers on
different threads.
