SELECT *, DATEDIFF(CURRENT_DATE(), delivered_date) AS days_since_delivered_date, UPPER(status) AS status_upper FROM shipments WHERE order_id = 23;
