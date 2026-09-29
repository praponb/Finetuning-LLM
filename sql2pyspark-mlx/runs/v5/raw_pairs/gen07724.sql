SELECT a.city, b.country FROM (SELECT DISTINCT city FROM customers) a CROSS JOIN (SELECT DISTINCT country FROM suppliers) b
