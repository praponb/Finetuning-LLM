WITH inventory_by_product AS (
    SELECT product_id, COUNT(*) AS cnt
    FROM inventory
    GROUP BY product_id
)
SELECT p.id, p.brand, ibp.cnt
FROM products p
LEFT JOIN inventory_by_product ibp ON p.id = ibp.product_id
WHERE ibp.cnt >= 9
ORDER BY p.brand
