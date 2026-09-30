SELECT
    city,
    is_active,
    consumer_credit_limit,
    unique_countrys
FROM (
    SELECT
        city,
        is_active,
        SUM(CASE WHEN segment = 'Consumer' THEN credit_limit ELSE 0 END) consumer_credit_limit,
        COUNT(DISTINCT age) unique_countrys
    FROM customers
    GROUP BY city, is_active
) t
WHERE unique_countrys >= 8
