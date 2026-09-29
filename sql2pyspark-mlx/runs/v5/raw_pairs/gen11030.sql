SELECT *, DATE_TRUNC('WEEK', delivered_date) AS delivered_date_period FROM shipments WHERE (cost BETWEEN 49 AND 88 AND status = 'RETURNED') OR order_id IN (2, 30);
