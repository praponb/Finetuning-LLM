SELECT QUARTER(launch_date) AS launch_quarter, COUNT(DISTINCT price) AS unique_categorys, COUNT(CASE WHEN brand = 'Globex' THEN 1 END) AS globex_count FROM products GROUP BY launch_quarter
