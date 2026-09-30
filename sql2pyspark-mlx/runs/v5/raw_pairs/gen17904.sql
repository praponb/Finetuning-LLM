SELECT id, brand, launch_date, cost, LAG(cost, 2) OVER (PARTITION BY brand ORDER BY launch_date, id) AS prev_cost FROM products WHERE stock_qty IS NOT NULL ORDER BY id DESC
