SELECT a.id, a.category, a.cost, a.price FROM products a WHERE a.cost = (SELECT MAX(b.cost) FROM products b WHERE b.category = a.category) ORDER BY a.id DESC;
