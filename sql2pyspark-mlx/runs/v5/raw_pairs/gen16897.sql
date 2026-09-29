SELECT id, device, customer_id, browser, CONCAT_WS('/', country, device) country_device_key FROM sessions WHERE started_at < '2025-06-15' ORDER BY id DESC
