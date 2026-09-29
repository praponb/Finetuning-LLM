SELECT x.city, y.region FROM (SELECT DISTINCT city FROM suppliers) x CROSS JOIN (SELECT DISTINCT region FROM stores) y ORDER BY city, region
