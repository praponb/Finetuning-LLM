SELECT a.country, b.city FROM (SELECT DISTINCT country FROM customers) a CROSS JOIN (SELECT DISTINCT city FROM stores) b
