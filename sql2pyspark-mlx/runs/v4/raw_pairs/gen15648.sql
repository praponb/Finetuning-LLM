SELECT * FROM (SELECT brand, category, stock_qty FROM products) PIVOT (AVG(stock_qty) FOR category IN ('Books', 'Sports', 'Electronics')) ORDER BY brand
