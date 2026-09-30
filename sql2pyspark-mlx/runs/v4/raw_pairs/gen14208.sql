SELECT MIN(shipped_date) AS first_shipped_date, ROUND((100 * SUM(CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_ups FROM shipments;
