SELECT * FROM (SELECT id, device_code, humidity, pressure, temp_c FROM device_readings) src UNPIVOT (val FOR metric IN (humidity, pressure, temp_c))
