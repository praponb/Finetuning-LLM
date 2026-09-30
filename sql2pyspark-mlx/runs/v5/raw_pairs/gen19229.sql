SELECT * FROM (SELECT QUARTER(opened_date) AS opened_quarter, region, sqft FROM stores) PIVOT (MAX(sqft) FOR region IN ('Central', 'North', 'West', 'South'));
