SELECT brand, MIN(price) AS min_price, MIN(cost) AS min_cost, COUNT(DISTINCT price) AS unique_suppliers FROM products WHERE stock_qty BETWEEN 111 AND 185 GROUP BY brand HAVING min_price > 470
