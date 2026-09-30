SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts DESC, id ASC) AS dedup_rank FROM device_readings QUALIFY dedup_rank = 1
