SELECT
    id,
    order_id,
    unit_price,
    RANK() OVER (PARTITION BY order_id ORDER BY unit_price DESC) unit_price_rank
FROM order_items
WHERE order_id IN (3, 5, 34, 38)
