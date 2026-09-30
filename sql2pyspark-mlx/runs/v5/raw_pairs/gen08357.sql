WITH inventory_by_product AS (
    SELECT product_id, ROUND(STDDEV(quantity), 2) stddev_quantity
    FROM inventory
    GROUP BY product_id
)
SELECT p.id, p.category, ibp.stddev_quantity
FROM products p
INNER JOIN inventory_by_product ibp ON p.id = ibp.product_id
ORDER BY category;
