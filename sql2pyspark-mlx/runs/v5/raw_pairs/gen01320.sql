SELECT id, launch_date, RANK() OVER (ORDER BY launch_date DESC) AS rnk FROM products
