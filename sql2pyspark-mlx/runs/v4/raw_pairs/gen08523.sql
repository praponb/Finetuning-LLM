WITH products_by_supplier AS (
    SELECT supplier_id, MIN(price) min_price
    FROM products
    GROUP BY supplier_id
)
SELECT suppliers.country, products_by_supplier.min_price
FROM suppliers
LEFT JOIN products_by_supplier ON suppliers.id = products_by_supplier.supplier_id
ORDER BY suppliers.country
