SELECT country, SUM(age) AS total_age, ROUND(SUM(age) * 100.0 / SUM(SUM(age)) OVER (), 2) AS pct_of_total FROM customers GROUP BY country ORDER BY total_age DESC, pct_of_total DESC
