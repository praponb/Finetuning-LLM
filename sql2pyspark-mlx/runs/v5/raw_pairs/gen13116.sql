SELECT x.segment, y.device FROM (SELECT DISTINCT segment FROM customers) x CROSS JOIN (SELECT DISTINCT device FROM sessions) y
