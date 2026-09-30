SELECT id, reading_ts FROM (SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY reading_ts DESC) AS dedup_rank FROM device_readings) t WHERE dedup_rank = 1
