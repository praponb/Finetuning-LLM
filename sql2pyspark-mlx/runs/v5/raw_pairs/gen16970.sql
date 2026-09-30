SELECT a.segment, b.country FROM (SELECT DISTINCT segment FROM customers) a CROSS JOIN (SELECT DISTINCT country FROM sessions) b
