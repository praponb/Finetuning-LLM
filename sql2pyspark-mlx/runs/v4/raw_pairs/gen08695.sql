SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC NULLS LAST, id) AS dedup_rank FROM device_readings QUALIFY dedup_rank = 1
