SELECT p.id, p.category, p.price, p.stock_qty FROM products p WHERE p.price < (SELECT AVG(p2.price) FROM products p2 WHERE p2.category = p.category) ORDER BY p.stock_qty;
