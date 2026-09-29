SELECT * FROM device_readings QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC, id) = 1
