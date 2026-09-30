SELECT x.city, y.region FROM (SELECT DISTINCT city FROM customers) x CROSS JOIN (SELECT DISTINCT region FROM stores) y;
