SELECT id, sqft, opened_date, region, LOWER(city) AS city_lower FROM stores WHERE city <> 'Singapore' AND region IN ('North', 'West', 'South') AND city IN ('Chiang Mai', 'London') ORDER BY sqft DESC;
