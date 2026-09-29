SELECT
    suppliers.id,
    suppliers.country,
    COUNT(products.id) product_count,
    SUM(CASE WHEN products.category = 'Books' THEN products.cost ELSE 0 END) books_cost,
    ROUND(AVG(products.price), 2) avg_price
FROM suppliers
LEFT JOIN products ON suppliers.id = products.supplier_id
WHERE suppliers.country = 'DE'
GROUP BY suppliers.id, suppliers.country
HAVING ROUND(AVG(products.price), 2) > 28
ORDER BY product_count DESC
