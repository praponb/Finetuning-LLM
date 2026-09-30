SELECT id, shipped_date, cost, delivered_date, DATEDIFF(delivered_date, shipped_date) AS days_shipped_date_to_delivered_date FROM shipments;
