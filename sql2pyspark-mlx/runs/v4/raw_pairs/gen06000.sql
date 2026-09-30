SELECT * FROM (SELECT brand, category, COUNT(DISTINCT stock_qty) unique_categorys FROM products GROUP BY brand, category) s WHERE unique_categorys >= 4 ORDER BY unique_categorys DESC, category
