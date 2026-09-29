SELECT id, brand, LEAST(cost, price) AS least_cost_price FROM products WHERE supplier_id IN (11, 19) ORDER BY least_cost_price DESC, brand
