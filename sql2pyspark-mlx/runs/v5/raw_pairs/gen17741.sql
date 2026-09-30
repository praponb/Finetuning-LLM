SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts DESC, id) AS latest_rank FROM device_readings QUALIFY latest_rank = 1;
