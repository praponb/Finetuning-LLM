SELECT x.city, y.browser FROM (SELECT DISTINCT city FROM customers) x CROSS JOIN (SELECT DISTINCT browser FROM sessions) y ORDER BY city, browser
