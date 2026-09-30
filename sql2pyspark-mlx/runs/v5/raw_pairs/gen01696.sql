SELECT YEAR(launch_date) AS launch_year, brand, COUNT(CASE WHEN brand = 'Acme' THEN 1 END) AS acme_count FROM products GROUP BY YEAR(launch_date), brand ORDER BY brand;
