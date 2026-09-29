SELECT id, city, sqft, region, RANK() OVER (PARTITION BY city ORDER BY sqft DESC) rnk, MIN(sqft) OVER (PARTITION BY region, city) region_min_sqft FROM stores WHERE opened_date < '2025-04-01'
