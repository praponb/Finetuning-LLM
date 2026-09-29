SELECT SUM(CASE WHEN region = 'West' THEN 1 ELSE 0 END) AS west_count, COUNT(DISTINCT sqft) AS unique_citys, COUNT(DISTINCT sqft) AS unique_regions
FROM stores
WHERE region NOT IN ('East', 'South', 'Central')
ORDER BY west_count;
