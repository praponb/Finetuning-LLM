SELECT id, credit_limit, email, LOWER(country) AS country_lower, LENGTH(name) AS name_length FROM customers WHERE segment IN ('Consumer', 'SMB') ORDER BY name_length DESC
