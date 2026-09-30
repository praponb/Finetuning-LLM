SELECT id, customer_id, TRIM(country) clean_country
FROM sessions
WHERE TO_DATE(started_at) < '2024-01-01'
ORDER BY id
LIMIT 20;
