SELECT * FROM (SELECT id, device_code, pressure, temp_c, humidity FROM device_readings) src UNPIVOT (value FOR metric_name IN (pressure, temp_c, humidity));
