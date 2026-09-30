SELECT
    a.balance,
    a.account_type,
    a.customer_id,
    c.city,
    CASE WHEN c.city IN ('New York', 'Singapore') THEN 'Core' ELSE 'Other' END AS city_group
FROM accounts a
INNER JOIN customers c ON c.id = a.customer_id
WHERE c.is_active;
