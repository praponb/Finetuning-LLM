SELECT device_code, firmware FROM device_readings EXCEPT ALL SELECT device_code, firmware FROM device_readings WHERE reading_ts >= '2024-05-08';
