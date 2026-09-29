WITH top_products AS (SELECT *, RANK() OVER (PARTITION BY brand ORDER BY launch_date DESC) AS rnk FROM products) SELECT id, brand, launch_date FROM top_products WHERE rnk <= 2 ORDER BY brand;
