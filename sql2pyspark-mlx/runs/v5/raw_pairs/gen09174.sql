SELECT carrier, ROUND(AVG(cost), 2) AS avg_cost, MAX(shipped_date) AS latest_shipped_date FROM shipments WHERE order_id IN (29, 31, 36) GROUP BY carrier ORDER BY carrier, latest_shipped_date
