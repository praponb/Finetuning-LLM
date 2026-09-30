SELECT device_code, AVG(pressure) AS avg_pressure, COUNT(DISTINCT id) AS distinct_ids, MIN(reading_ts) AS first_seen FROM device_readings GROUP BY device_code ORDER BY device_code
