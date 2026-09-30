WITH products_by_supplier AS (
    SELECT supplier_id, SUM(CASE WHEN category = 'Books' THEN 1 ELSE 0 END) AS books_count, ROUND(STDDEV(cost), 2) AS stddev_cost
    FROM products
    WHERE product_name IS NOT NULL
    GROUP BY supplier_id
)
SELECT
    suppliers.rating,
    suppliers.city,
    products_by_supplier.books_count,
    products_by_supplier.stddev_cost
FROM suppliers
JOIN products_by_supplier ON suppliers.id = products_by_supplier.supplier_id
ORDER BY books_count, suppliers.rating DESC
