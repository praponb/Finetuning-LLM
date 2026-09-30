SELECT id, category, cost FROM (SELECT id, category, cost, RANK() OVER (PARTITION BY category ORDER BY cost) row_num FROM products) ranked WHERE row_num <= 2
