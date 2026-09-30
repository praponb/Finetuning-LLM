SELECT * FROM (SELECT id, device_code, pressure, humidity, temp_c FROM device_readings) src UNPIVOT (value FOR metric IN (pressure, humidity, temp_c))
