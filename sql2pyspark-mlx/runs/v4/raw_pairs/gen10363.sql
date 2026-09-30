SELECT * FROM (SELECT brand, category, stock_qty FROM products)
PIVOT (COUNT(stock_qty) FOR category IN ('Home', 'Grocery', 'Electronics'))
