SELECT cost, order_id, shipped_date, DATE_FORMAT(shipped_date, 'yyyy-MM') AS shipped_date_label, DATE_FORMAT(shipped_date, 'MMM yyyy') AS shipped_date_label_2 FROM shipments
