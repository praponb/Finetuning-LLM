WITH SupplierTotals AS (
    SELECT supplier_id, ROUND(STDDEV(stock_qty), 2) AS stddev_stock_qty
    FROM products
    GROUP BY supplier_id
)
SELECT
    suppliers.id,
    suppliers.supplier_name,
    suppliers.rating,
    SupplierTotals.stddev_stock_qty
FROM suppliers
INNER JOIN SupplierTotals ON suppliers.id = SupplierTotals.supplier_id
WHERE SupplierTotals.stddev_stock_qty >= 26;
