SELECT id, started_at, browser, country, INITCAP(TRIM(device)) AS device_title FROM sessions WHERE customer_id IS NOT NULL;
