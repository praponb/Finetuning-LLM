SELECT * FROM (SELECT id, device_code, pressure, humidity, temp_c FROM device_readings) src UNPIVOT INCLUDE NULLS (val FOR measure IN (pressure AS PRESSURE_VAL, humidity, temp_c AS TEMP_C_VAL))
