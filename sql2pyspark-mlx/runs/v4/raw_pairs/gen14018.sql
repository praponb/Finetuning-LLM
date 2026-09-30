SELECT
    id,
    quantity,
    order_id,
    unit_price,
    CAST(unit_price AS INT) unit_price_int
FROM order_items
