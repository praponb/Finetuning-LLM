SELECT * FROM (SELECT QUARTER(opened_date) AS opened_quarter, city, sqft FROM stores) PIVOT (SUM(sqft) FOR city IN ('Bangkok', 'Singapore')) ORDER BY opened_quarter;
