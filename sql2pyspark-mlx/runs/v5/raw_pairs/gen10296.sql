SELECT * FROM (SELECT brand, category, stock_qty FROM products) PIVOT (AVG(stock_qty) FOR category IN ('Home', 'Books', 'Grocery', 'Sports')) ORDER BY brand
