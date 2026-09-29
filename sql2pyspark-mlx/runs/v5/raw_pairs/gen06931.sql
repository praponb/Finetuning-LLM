SELECT s.id, s.opened_date, s.region, s.city FROM stores s WHERE s.sqft <= 7467 AND s.id IN (SELECT store_id FROM inventory WHERE last_restock BETWEEN '2023-03-23' AND '2024-04-01')
