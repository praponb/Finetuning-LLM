SELECT * FROM (SELECT brand, category, price FROM products)
PIVOT (MAX(price) FOR category IN ('Electronics', 'Books', 'Fashion', 'Grocery'))
