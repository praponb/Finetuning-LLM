SELECT a.city, b.country FROM (SELECT DISTINCT city FROM suppliers) a CROSS JOIN (SELECT DISTINCT country FROM sessions) b ORDER BY city, country
