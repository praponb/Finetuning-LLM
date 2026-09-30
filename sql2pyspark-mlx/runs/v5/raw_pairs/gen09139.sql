SELECT shipped_date FROM shipments INTERSECT SELECT end_date FROM subscriptions WHERE MONTH(start_date) IN (2, 7, 8) ORDER BY shipped_date DESC;
