SELECT x.country, y.browser FROM (SELECT DISTINCT country FROM suppliers) x CROSS JOIN (SELECT DISTINCT browser FROM sessions) y ORDER BY country, browser;
