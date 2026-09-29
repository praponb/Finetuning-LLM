SELECT
    id,
    age,
    segment,
    name
FROM customers
WHERE segment = 'Corporate' OR is_active
ORDER BY age DESC;
