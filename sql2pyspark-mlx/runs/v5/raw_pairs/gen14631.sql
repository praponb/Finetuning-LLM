SELECT brand, AVG(cost) AS avg_cost FROM products WHERE NOT ((launch_date < '2023-12-01' AND category IN ('Home', 'Fashion')) OR cost BETWEEN 550 AND 710) GROUP BY brand;
