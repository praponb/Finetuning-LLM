SELECT status, COUNT(CASE WHEN status = 'IN_TRANSIT' THEN 1 END) AS in_transit_count, MIN(shipped_date) AS first_shipped_date FROM shipments GROUP BY status HAVING COUNT(*) >= 3
