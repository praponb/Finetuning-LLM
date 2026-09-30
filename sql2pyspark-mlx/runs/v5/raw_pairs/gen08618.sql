SELECT
    c.id,
    c.email,
    c.age,
    t.channel_values
FROM customers c
INNER JOIN (SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) channel_values
    FROM orders
    WHERE shipping_fee >= 39
    GROUP BY customer_id) t ON c.id = t.customer_id
ORDER BY channel_values
