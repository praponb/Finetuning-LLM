SELECT
    id,
    order_id,
    status,
    delivered_date,
    carrier,
    cost,
    NTILE(2) OVER (PARTITION BY order_id, status ORDER BY delivered_date, id) AS delivered_date_ntile,
    LAG(cost, 1) OVER (PARTITION BY carrier ORDER BY delivered_date, id) AS prev_cost
FROM shipments;
