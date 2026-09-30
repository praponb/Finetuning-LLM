SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS dedup_rank FROM device_readings) x WHERE dedup_rank = 1
