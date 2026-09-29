SELECT status, DATE_TRUNC('MONTH', shipped_date) AS shipped_month, MIN(shipped_date) AS first_shipped_date
FROM shipments
WHERE status = 'RETURNED'
GROUP BY status, DATE_TRUNC('MONTH', shipped_date)
ORDER BY first_shipped_date ASC;
