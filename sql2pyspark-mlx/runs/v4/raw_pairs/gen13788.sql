SELECT id, cost, status, shipped_date, TRIM(status) AS clean_status, DATE_ADD(delivered_date, 21) AS delivered_date_plus_21d FROM shipments ORDER BY clean_status
