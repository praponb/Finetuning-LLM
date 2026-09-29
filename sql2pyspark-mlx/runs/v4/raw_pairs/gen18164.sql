SELECT * FROM (SELECT MONTH(opened_date) AS opened_month, city, sqft FROM stores WHERE city = 'Tokyo') PIVOT (MAX(sqft) FOR city IN ('London', 'Bangkok'));
