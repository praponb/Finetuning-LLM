SELECT id, sqft FROM stores WHERE region IN ('South', 'West', 'Central') INTERSECT SELECT id, sqft FROM stores WHERE city != 'Chiang Mai'
