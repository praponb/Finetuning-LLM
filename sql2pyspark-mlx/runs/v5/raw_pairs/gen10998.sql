SELECT order_id, MAX(cost) AS max_cost, AVG(cost) AS avg_cost FROM shipments GROUP BY 1 HAVING SUM(CASE WHEN carrier = 'FedEx' THEN cost ELSE 0 END) >= 76;
