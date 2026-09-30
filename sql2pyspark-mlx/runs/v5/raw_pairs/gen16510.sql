SELECT id, customer_id, started_at, country, INITCAP(TRIM(country)) country_title, DATE_FORMAT(started_at, 'yyyy-MM-dd') started_at_label FROM sessions ORDER BY customer_id DESC, id LIMIT 10
