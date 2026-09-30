SELECT carrier, COUNT(*) AS total_count, MIN(shipped_date) AS first_shipped_date, COUNT(*) AS row_count FROM shipments WHERE MONTH(shipped_date) IN (2, 4, 6) GROUP BY carrier
