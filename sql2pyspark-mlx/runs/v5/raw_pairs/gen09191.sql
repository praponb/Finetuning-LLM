SELECT id, carrier, cost, status, CONCAT(status, ', ', carrier) AS status_carrier FROM shipments WHERE cost < 75 AND delivered_date BETWEEN '2023-03-01' AND '2023-10-01'
