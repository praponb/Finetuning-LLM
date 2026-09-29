SELECT id, carrier, delivered_date, DATEDIFF(delivered_date, shipped_date) AS days_shipped_date_to_delivered_date, INITCAP(TRIM(status)) AS status_title FROM shipments
