SELECT s.region, AVG(o.amount) AS avg_amount, SUM(CASE WHEN o.channel = 'mobile' THEN 1 ELSE 0 END) AS mobile_count FROM orders o JOIN stores s ON o.store_id = s.id GROUP BY s.region;
