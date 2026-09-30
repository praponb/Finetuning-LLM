SELECT * FROM (SELECT brand, category, cost FROM products WHERE YEAR(launch_date) >= 2023) PIVOT (AVG(cost) FOR category IN ('Books', 'Home')) ORDER BY brand;
