SELECT id, cost, shipped_date, delivered_date, DATE_TRUNC('month', shipped_date) AS shipped_date_period FROM shipments WHERE order_id = 30;
