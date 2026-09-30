SELECT carrier, MAX(shipped_date) AS latest_shipped_date, MIN(shipped_date) AS first_shipped_date
FROM shipments
GROUP BY carrier
ORDER BY carrier, latest_shipped_date
LIMIT 10;
