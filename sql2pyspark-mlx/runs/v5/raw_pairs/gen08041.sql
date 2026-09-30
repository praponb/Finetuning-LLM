SELECT * FROM (SELECT brand, category, stock_qty FROM products)
PIVOT (MAX(stock_qty) FOR category IN ('Fashion', 'Grocery', 'Electronics', 'Books'))
ORDER BY brand
