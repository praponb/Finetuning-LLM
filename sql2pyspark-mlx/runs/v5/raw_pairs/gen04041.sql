SELECT id, customer_id, browser, LOWER(device) AS device_lower, TRIM(country) AS clean_country FROM sessions;
