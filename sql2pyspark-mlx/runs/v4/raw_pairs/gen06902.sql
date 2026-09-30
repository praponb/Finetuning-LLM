SELECT * FROM (SELECT id, device_code, humidity, pressure FROM device_readings) src UNPIVOT INCLUDE NULLS (value FOR measure IN (humidity, pressure))
