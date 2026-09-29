SELECT *, RANK() OVER (PARTITION BY brand, category ORDER BY stock_qty) AS row_num
FROM products
QUALIFY row_num = 1;
