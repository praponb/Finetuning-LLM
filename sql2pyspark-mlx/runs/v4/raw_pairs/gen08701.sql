SELECT * FROM (SELECT id, device_code, humidity, temp_c, pressure FROM device_readings) src UNPIVOT (metric_value FOR metric_name IN (humidity, temp_c AS TEMP_C, pressure));
