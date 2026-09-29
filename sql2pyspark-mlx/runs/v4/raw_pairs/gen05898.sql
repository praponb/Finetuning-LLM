SELECT id, status, shipped_date FROM (SELECT id, status, shipped_date, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date DESC, id) AS rnk FROM shipments) AS ranked WHERE rnk <= 5
