SELECT id, order_id, shipped_date, PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY shipped_date) AS shipped_date_pct_rank FROM shipments ORDER BY shipped_date DESC, order_id;
