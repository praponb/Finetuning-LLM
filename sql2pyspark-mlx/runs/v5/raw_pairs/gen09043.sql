SELECT
    id,
    product_id,
    unit_price,
    MAX(unit_price) OVER (PARTITION BY product_id) product_id_max_unit_price
FROM order_items
