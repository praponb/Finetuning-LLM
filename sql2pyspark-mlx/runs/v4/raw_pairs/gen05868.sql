SELECT id, brand, stock_qty FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty DESC, id) AS rnk FROM products WHERE brand = 'Globex') AS r WHERE rnk <= 5 ORDER BY 1 DESC
