SELECT * FROM (SELECT id, device_code, pressure, temp_c FROM device_readings) src UNPIVOT INCLUDE NULLS (metric_value FOR measure IN (pressure AS PRESSURE, temp_c));
