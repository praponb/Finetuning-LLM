SELECT id, carrier, delivered_date, RANK() OVER (PARTITION BY carrier ORDER BY delivered_date DESC) AS rnk FROM shipments
