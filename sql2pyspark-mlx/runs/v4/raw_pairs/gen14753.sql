SELECT
    id,
    order_id,
    unit_price,
    quantity,
    ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY order_id), 3) AS unit_price_share,
    NTILE(3) OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS bucket
FROM order_items
ORDER BY unit_price_share
