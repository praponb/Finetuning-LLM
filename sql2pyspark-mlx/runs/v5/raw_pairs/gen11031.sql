SELECT
    a.balance,
    a.status,
    a.customer_id,
    c.city,
    c.name,
    CASE WHEN c.segment IN ('Corporate', 'Consumer') THEN 'Priority' ELSE 'Other' END AS segment_group
FROM accounts a
LEFT OUTER JOIN customers c ON a.customer_id = c.id
ORDER BY segment_group DESC;
