WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty DESC, id) AS rnk FROM products) SELECT * FROM numbered WHERE rnk <= 3 ORDER BY price DESC;
