WITH supplier_summary AS (
    SELECT supplier_id, MIN(price) AS min_price
    FROM products
    GROUP BY supplier_id
)
SELECT suppliers.id, suppliers.supplier_name, supplier_summary.min_price
FROM suppliers
LEFT JOIN supplier_summary ON suppliers.id = supplier_summary.supplier_id;
