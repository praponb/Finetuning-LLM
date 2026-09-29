SELECT brand, MIN(cost) AS min_cost FROM products WHERE category = 'Electronics' AND launch_date BETWEEN '2023-09-01' AND '2025-07-01' GROUP BY brand HAVING SUM(stock_qty) >= 700;
