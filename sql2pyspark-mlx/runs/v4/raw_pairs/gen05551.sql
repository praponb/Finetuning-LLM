SELECT
    i.id,
    i.last_restock,
    i.store_id,
    s.region,
    s.store_name,
    p.product_name,
    p.brand,
    ROUND(p.cost / p.price, 2) AS margin
FROM inventory i
LEFT JOIN stores s ON i.store_id = s.id
RIGHT JOIN products p ON p.id = i.product_id
WHERE p.stock_qty BETWEEN 193 AND 265 AND p.category IN ('Home', 'Fashion', 'Grocery')
