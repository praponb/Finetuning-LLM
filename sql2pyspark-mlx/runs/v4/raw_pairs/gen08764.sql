SELECT id, price, ROW_NUMBER() OVER (ORDER BY price DESC, id) AS row_num
FROM products
WHERE stock_qty != 225
ORDER BY row_num;
