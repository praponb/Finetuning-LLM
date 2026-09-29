SELECT id, started_at, country, customer_id, SUBSTRING(country, 1, 3) country_prefix FROM sessions WHERE country = 'SG';
