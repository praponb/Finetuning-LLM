SELECT
    c.id,
    c.city,
    c.country,
    t.device_list
FROM customers c
LEFT JOIN (SELECT customer_id, SORT_ARRAY(COLLECT_SET(device)) AS device_list
    FROM sessions
    GROUP BY customer_id) t ON c.id = t.customer_id;
