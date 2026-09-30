SELECT is_active, signup_date, CAST(age AS DOUBLE) AS age_str FROM customers WHERE NOT is_active AND (name LIKE 'K%' OR segment = 'Corporate')
