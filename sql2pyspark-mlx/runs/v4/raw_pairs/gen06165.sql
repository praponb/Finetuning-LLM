SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c DESC, id) AS latest_rank FROM device_readings) ranked WHERE latest_rank = 1;
