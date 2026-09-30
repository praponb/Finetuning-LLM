SELECT * FROM (SELECT id, device_code, temp_c, pressure FROM device_readings) src UNPIVOT (reading FOR measure IN (temp_c, pressure))
