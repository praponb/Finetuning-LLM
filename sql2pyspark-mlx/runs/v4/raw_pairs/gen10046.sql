SELECT
    c.id,
    c.name,
    COUNT(o.id) AS order_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(o.status))) AS status_values
FROM customers c
JOIN orders o ON c.id = o.customer_id
WHERE c.credit_limit < 12200
GROUP BY c.id, c.name;
