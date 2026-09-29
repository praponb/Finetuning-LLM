SELECT channel, COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count, COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count FROM orders GROUP BY channel HAVING mobile_count < 2;
