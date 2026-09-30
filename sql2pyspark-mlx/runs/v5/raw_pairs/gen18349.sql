SELECT carrier, ROUND((100 * SUM(CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_ups, SUM(cost) AS total_cost FROM shipments GROUP BY carrier ORDER BY pct_ups DESC;
