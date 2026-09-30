SELECT SUM(CASE WHEN country = 'SG' THEN rating ELSE 0 END) AS sg_rating, ROUND((100 * SUM(CASE WHEN country = 'DE' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_de FROM suppliers ORDER BY sg_rating DESC
