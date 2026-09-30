SELECT x.city, y.device FROM (SELECT DISTINCT city FROM customers) x CROSS JOIN (SELECT DISTINCT device FROM sessions) y;
