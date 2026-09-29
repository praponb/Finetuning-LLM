SELECT id, country, LOWER(country) AS country_lower, CAST(started_at AS DATE) AS started_at_day FROM sessions WHERE started_at >= '2024-03-01' AND customer_id IN (4, 24);
