SELECT
    customers.country,
    customers.is_active,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(sessions.device))) AS device_values,
    COUNT(CASE WHEN sessions.browser = 'Safari' THEN 1 END) AS safari_count
FROM sessions
INNER JOIN customers ON sessions.customer_id = customers.id
WHERE sessions.country NOT IN ('TH', 'SG')
GROUP BY customers.country, customers.is_active;
