SELECT shipped_date, delivered_date, INITCAP(TRIM(status)) AS status_title FROM shipments WHERE status != 'DELIVERED';
