SELECT id, device, customer_id, started_at, TRIM(country) AS clean_country, UPPER(country) AS country_upper FROM sessions ORDER BY 2, 4;
