SELECT a.id, a.category, a.stock_qty FROM products a WHERE a.stock_qty < (SELECT AVG(b.stock_qty) FROM products b WHERE b.category = a.category) ORDER BY a.stock_qty, a.id DESC
