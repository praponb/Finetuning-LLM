WITH supplier_totals AS (
    SELECT supplier_id, SUM(CASE WHEN brand = 'Globex' THEN stock_qty ELSE 0 END) globex_stock_qty
    FROM products
    GROUP BY supplier_id
)
SELECT
    suppliers.id,
    suppliers.city,
    suppliers.supplier_name,
    supplier_totals.globex_stock_qty
FROM suppliers
INNER JOIN supplier_totals ON suppliers.id = supplier_totals.supplier_id
ORDER BY suppliers.supplier_name DESC;
