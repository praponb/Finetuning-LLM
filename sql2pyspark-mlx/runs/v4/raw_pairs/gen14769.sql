SELECT id, cost, brand, launch_date FROM products WHERE cost > (SELECT AVG(cost) FROM products) ORDER BY launch_date
