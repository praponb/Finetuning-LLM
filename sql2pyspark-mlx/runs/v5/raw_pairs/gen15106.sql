SELECT brand, min_cost FROM (SELECT brand, MIN(cost) AS min_cost FROM products WHERE cost > 300 GROUP BY brand) AS x WHERE min_cost >= 210 ORDER BY min_cost DESC
