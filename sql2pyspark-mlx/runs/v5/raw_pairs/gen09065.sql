SELECT a.id, a.category, a.price, a.cost FROM products a WHERE a.price > (SELECT AVG(b.price) FROM products b WHERE b.category = a.category) ORDER BY a.category, a.id;
