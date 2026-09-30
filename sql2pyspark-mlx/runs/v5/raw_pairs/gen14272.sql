SELECT
    id,
    is_active,
    segment,
    GREATEST(credit_limit, age) AS greatest_credit_limit_age
FROM customers
WHERE name LIKE '%Hen%';
