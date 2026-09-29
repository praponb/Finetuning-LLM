SELECT
    id,
    product_id,
    quantity,
    ROUND(quantity, 1) AS quantity_rounded,
    quantity * unit_price AS line_total
FROM order_items
WHERE unit_price <= 370 AND (unit_price > 350 OR product_id IS NOT NULL);
