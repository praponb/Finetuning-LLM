SELECT x.city, y.device FROM (SELECT DISTINCT city FROM suppliers) x CROSS JOIN (SELECT DISTINCT device FROM sessions) y ORDER BY city, device
