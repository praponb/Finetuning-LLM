WITH products_by_supplier AS (
    SELECT supplier_id, ROUND(STDDEV(cost), 2) AS stddev_cost
    FROM products
    GROUP BY supplier_id
)
SELECT suppliers.id, suppliers.supplier_name, products_by_supplier.stddev_cost
FROM suppliers
INNER JOIN products_by_supplier ON suppliers.id = products_by_supplier.supplier_id
WHERE products_by_supplier.stddev_cost >= 60
ORDER BY stddev_cost, supplier_name DESC;
