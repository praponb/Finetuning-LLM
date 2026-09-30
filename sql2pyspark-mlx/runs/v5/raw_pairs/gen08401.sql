SELECT s.id, s.city, s.sqft FROM stores s WHERE s.sqft = (SELECT MIN(s2.sqft) FROM stores s2 WHERE s2.city = s.city)
