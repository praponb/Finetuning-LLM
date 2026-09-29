SELECT id, category, cost FROM (SELECT id, category, cost, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) pos FROM products) t WHERE pos <= 2
