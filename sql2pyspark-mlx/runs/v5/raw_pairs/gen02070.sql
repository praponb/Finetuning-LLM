SELECT
    id,
    name,
    signup_date,
    credit_limit,
    CASE WHEN city IN ('Bangkok', 'New York') THEN 'Group A' ELSE 'Other' END AS city_group
FROM customers
WHERE NOT is_active;
