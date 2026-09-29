SELECT
    id,
    order_id,
    delivered_date,
    RANK() OVER (PARTITION BY order_id ORDER BY delivered_date) rnk
FROM shipments;
