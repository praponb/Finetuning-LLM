SELECT id, region, city, opened_date, SUBSTRING(region, 1, 2) region_prefix FROM stores WHERE region IN ('South', 'North', 'West') ORDER BY opened_date
