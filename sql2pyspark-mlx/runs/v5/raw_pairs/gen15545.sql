SELECT id, started_at, SUBSTRING(device, 1, 2) AS device_prefix, INITCAP(TRIM(device)) AS device_title FROM sessions WHERE customer_id IS NOT NULL ORDER BY device_prefix DESC;
