SELECT *, LOWER(region) region_lower
FROM stores
WHERE NOT (region IS NULL AND (city NOT IN ('Tokyo', 'London', 'New York') OR region IN ('North', 'East', 'West')))
ORDER BY city DESC, region;
