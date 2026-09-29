WITH product_totals AS (
    SELECT product_id, COUNT(*) AS cnt, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM inventory
    WHERE store_id = 6
    GROUP BY product_id
)
SELECT
    products.id,
    products.product_name,
    products.brand,
    product_totals.cnt,
    product_totals.stddev_quantity
FROM products
JOIN product_totals ON products.id = product_totals.product_id;
