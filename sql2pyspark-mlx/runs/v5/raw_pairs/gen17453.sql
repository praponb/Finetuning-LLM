SELECT id, brand, launch_date, RANK() OVER (PARTITION BY brand ORDER BY launch_date DESC) AS rnk FROM products ORDER BY brand;
