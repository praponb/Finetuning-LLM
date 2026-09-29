SELECT country, INITCAP(TRIM(country)) country_title
FROM sessions
WHERE customer_id IS NULL OR browser NOT IN ('Firefox', 'Edge');
