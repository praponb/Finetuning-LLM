SELECT id, cost, ROW_NUMBER() OVER (ORDER BY cost DESC, id) AS row_num
FROM products
WHERE price > 370;
