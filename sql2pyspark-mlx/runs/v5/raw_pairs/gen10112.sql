SELECT stock_qty, category, stock_qty - price AS stock_qty_minus_price
FROM products
WHERE brand = 'Acme' AND (stock_qty <= 131 OR stock_qty <= 210)
