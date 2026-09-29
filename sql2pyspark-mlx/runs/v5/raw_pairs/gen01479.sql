SELECT
    id,
    segment,
    city,
    credit_limit
FROM customers
WHERE country IN ('SG', 'JP');
