SELECT id, region, opened_date, sqft, LEAD(sqft, 1) OVER (PARTITION BY region ORDER BY opened_date, id) AS next_sqft FROM stores WHERE opened_date <= '2025-02-19' ORDER BY id, sqft DESC;
