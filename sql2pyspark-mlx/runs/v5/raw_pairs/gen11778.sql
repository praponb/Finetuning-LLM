WITH ProductsBySupplier AS (
    SELECT supplier_id, MAX(cost) AS max_cost
    FROM products
    GROUP BY supplier_id
)
SELECT suppliers.city, ProductsBySupplier.max_cost
FROM suppliers
INNER JOIN ProductsBySupplier ON suppliers.id = ProductsBySupplier.supplier_id
WHERE ProductsBySupplier.max_cost >= 620
