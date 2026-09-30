SELECT x.country, y.browser FROM (SELECT DISTINCT country FROM customers) x CROSS JOIN (SELECT DISTINCT browser FROM sessions) y
