SELECT id, is_active, TRIM(country) AS clean_country
FROM customers
WHERE (segment = 'Consumer' AND UPPER(email) LIKE '%VY@%') OR LOWER(name) LIKE '%aro%';
