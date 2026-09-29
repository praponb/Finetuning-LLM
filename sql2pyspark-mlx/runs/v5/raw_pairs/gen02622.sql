SELECT *, LENGTH(carrier) AS carrier_length, DATE_TRUNC('MONTH', delivered_date) AS delivered_date_period FROM shipments ORDER BY order_id, carrier_length DESC
