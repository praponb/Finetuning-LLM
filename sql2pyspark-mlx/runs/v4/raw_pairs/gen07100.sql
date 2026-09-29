SELECT
    id,
    order_id,
    quantity,
    COALESCE(quantity, 0) AS quantity_filled
FROM order_items
WHERE unit_price <= 260
