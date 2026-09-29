SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c DESC NULLS LAST, id) AS rank_in_group FROM device_readings) ranked WHERE rank_in_group = 1;
