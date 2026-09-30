SELECT * FROM (SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, region, sqft FROM stores) PIVOT (MAX(sqft) FOR region IN ('West', 'Central', 'South', 'East'))
