SELECT
    c.id,
    c.age,
    t.open_count,
    t.account_type_values
FROM customers c
LEFT JOIN (SELECT customer_id, SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) open_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) account_type_values
    FROM accounts
    WHERE status IS NULL
    GROUP BY customer_id) t ON c.id = t.customer_id
ORDER BY 3 DESC
