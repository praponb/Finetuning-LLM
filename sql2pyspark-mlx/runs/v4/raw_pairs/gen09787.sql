SELECT
    id,
    product_name,
    category,
    cost,
    ROUND(cost / price, 2) margin
FROM products
WHERE category IN ('Grocery', 'Fashion', 'Electronics') OR launch_date >= '2025-10-05'
ORDER BY product_name DESC, margin DESC
