SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date DESC, id ASC) AS latest_rank FROM shipments QUALIFY latest_rank = 1
