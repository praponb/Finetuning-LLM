SELECT id, browser, customer_id, INITCAP(TRIM(country)) AS country_title FROM sessions WHERE country <> 'US' AND browser = 'Edge' AND customer_id IN (4, 13);
