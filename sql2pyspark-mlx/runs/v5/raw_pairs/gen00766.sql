SELECT a.id, a.country, a.rating FROM suppliers a WHERE a.rating = (SELECT MAX(b.rating) FROM suppliers b WHERE b.country = a.country) ORDER BY a.id;
