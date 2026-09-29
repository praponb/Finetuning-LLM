SELECT
    id,
    quantity,
    product_id,
    CAST(quantity AS DOUBLE) AS quantity_str,
    ROUND(quantity / unit_price, 2) AS line_total
FROM order_items
WHERE unit_price <= 150 AND order_id IN (20, 21, 34);
