SELECT status, COUNT(CASE WHEN channel = 'partner' THEN 1 END) AS partner_count FROM orders GROUP BY status
