SELECT id, device_code, humidity FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure NULLS LAST, id) AS rank_in_group FROM device_readings) t WHERE rank_in_group = 1
