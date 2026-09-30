SELECT p.id, p.category, p.cost, p.launch_date FROM products p WHERE p.cost = (SELECT MAX(p2.cost) FROM products p2 WHERE p2.category = p.category)
