SELECT id, opened_date, region, sqft FROM stores WHERE opened_date >= (SELECT MAX(opened_date) FROM stores) ORDER BY 2 DESC
