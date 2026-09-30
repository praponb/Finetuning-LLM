SELECT id, category, stock_qty FROM (SELECT *, RANK() OVER (PARTITION BY category ORDER BY stock_qty DESC) AS row_num FROM products WHERE price < 1090) AS r WHERE row_num <= 5;
