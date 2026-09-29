SELECT region, COUNT(*) AS row_count FROM stores WHERE (store_name LIKE '%Old%' AND city <> 'Berlin') OR city IN ('Chiang Mai', 'Bangkok') GROUP BY region;
