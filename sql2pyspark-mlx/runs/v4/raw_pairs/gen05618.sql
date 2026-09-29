SELECT * FROM (SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, region, sqft FROM stores) PIVOT (MAX(sqft) FOR region IN ('South', 'East', 'West', 'North')) ORDER BY opened_month;
