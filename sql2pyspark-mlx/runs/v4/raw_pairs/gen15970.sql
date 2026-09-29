SELECT
    suppliers.id,
    suppliers.city,
    COUNT(products.id) product_count,
    SUM(CASE WHEN products.brand = 'Globex' THEN products.stock_qty ELSE 0 END) globex_stock_qty,
    COUNT(DISTINCT products.price) unique_suppliers
FROM suppliers
LEFT JOIN products ON products.supplier_id = suppliers.id
GROUP BY suppliers.id, suppliers.city
HAVING COUNT(DISTINCT products.price) >= 5
ORDER BY 5 DESC
