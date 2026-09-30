SELECT DATE_FORMAT(shipped_date, 'yyyy-MM') shipped_month, COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) delivered_count FROM shipments GROUP BY 1 ORDER BY shipped_month;
