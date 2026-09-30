SELECT shipped_date FROM shipments WHERE status = 'IN_TRANSIT' INTERSECT SELECT end_date FROM subscriptions WHERE YEAR(start_date) >= 2023 ORDER BY shipped_date
