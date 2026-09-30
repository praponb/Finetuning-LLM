SELECT * FROM (SELECT id, device_code, pressure, temp_c, humidity FROM device_readings) src UNPIVOT (reading FOR metric IN (pressure, temp_c, humidity));
