SELECT * FROM (SELECT brand, category, stock_qty FROM products) src PIVOT (AVG(stock_qty) FOR category IN ('Home', 'Electronics'))
