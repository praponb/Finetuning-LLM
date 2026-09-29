SELECT *, TO_DATE(started_at) AS started_at_date, TRIM(device) AS clean_device
FROM sessions
WHERE customer_id IS NULL AND started_at >= '2023-04-01' AND customer_id IS NOT NULL;
