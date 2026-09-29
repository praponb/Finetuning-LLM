SELECT x.country, y.device FROM (SELECT DISTINCT country FROM customers) x CROSS JOIN (SELECT DISTINCT device FROM sessions) y ORDER BY country, device;
