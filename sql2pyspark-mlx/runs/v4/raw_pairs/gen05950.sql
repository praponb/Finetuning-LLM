SELECT *, DATE_TRUNC('year', shipped_date) shipped_date_period FROM shipments WHERE status = 'RETURNED';
