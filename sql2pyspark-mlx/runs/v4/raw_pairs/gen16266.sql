SELECT id, city, opened_date, sqft, LAG(sqft, 2) OVER (PARTITION BY city ORDER BY opened_date, id) prev_sqft FROM stores WHERE opened_date >= '2024-03-01' ORDER BY sqft
