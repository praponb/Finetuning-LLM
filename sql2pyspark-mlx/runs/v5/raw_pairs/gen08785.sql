SELECT id, product_id, ROUND(quantity * 0.85, 2) AS quantity_adjusted
FROM order_items
WHERE unit_price <= 190 AND (product_id IS NOT NULL OR unit_price > 210)
ORDER BY id DESC
LIMIT 50;
