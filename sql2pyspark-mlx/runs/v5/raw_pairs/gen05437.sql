SELECT brand, category, COUNT(DISTINCT stock_qty) AS unique_suppliers
FROM products
WHERE stock_qty > 284 AND cost < 240 AND stock_qty != 349
GROUP BY brand, category;
