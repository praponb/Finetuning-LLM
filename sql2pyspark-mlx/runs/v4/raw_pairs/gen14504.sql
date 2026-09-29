SELECT * FROM (SELECT id, device_code, pressure, temp_c FROM device_readings) src UNPIVOT INCLUDE NULLS (value FOR kpi IN (pressure, temp_c))
