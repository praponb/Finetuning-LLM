SELECT delivered_date, shipped_date, carrier, LENGTH(status) AS status_length FROM shipments ORDER BY shipped_date;
