SELECT
    id,
    email,
    name,
    credit_limit
FROM customers
WHERE name NOT LIKE '%smith%';
