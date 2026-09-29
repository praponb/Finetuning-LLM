SELECT id, stock_qty, RANK() OVER (ORDER BY stock_qty DESC) stock_qty_rank
FROM products;
