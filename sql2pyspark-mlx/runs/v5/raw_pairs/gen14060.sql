SELECT
    c.id,
    c.signup_date,
    c.segment,
    t.channel_values,
    t.cnt
FROM customers c
INNER JOIN (SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values, COUNT(*) AS cnt
    FROM orders
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.cnt >= 8
ORDER BY channel_values ASC;
