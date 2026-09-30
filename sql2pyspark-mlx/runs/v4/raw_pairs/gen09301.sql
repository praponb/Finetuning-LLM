SELECT id, product_name, ROUND(cost * 1.15, 2) cost_adjusted
FROM products
WHERE supplier_id IS NULL AND product_name IS NOT NULL
ORDER BY id, cost_adjusted DESC
