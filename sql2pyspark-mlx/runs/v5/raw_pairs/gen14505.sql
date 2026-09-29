select * from (select id, device_code, pressure, humidity, temp_c from device_readings) src unpivot (reading for metric in (pressure, humidity as HUMIDITY_VAL, temp_c)) long_rows;
