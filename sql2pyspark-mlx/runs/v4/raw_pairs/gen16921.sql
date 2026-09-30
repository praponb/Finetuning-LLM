SELECT
    o.id,
    o.shipping_fee,
    o.channel,
    t.max_cost,
    t.carrier_list
FROM orders o
INNER JOIN (SELECT order_id, MAX(cost) AS max_cost, SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list
    FROM shipments
    GROUP BY order_id) t ON o.id = t.order_id
ORDER BY max_cost
