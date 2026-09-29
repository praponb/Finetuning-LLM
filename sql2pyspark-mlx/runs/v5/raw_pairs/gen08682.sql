SELECT
    CASE WHEN units >= 33 THEN 'high' WHEN units >= 19 THEN 'mid' ELSE 'low' END AS units_bucket,
    store_id,
    CASE WHEN revenue >= 4400 THEN 'high' WHEN revenue >= 1100 THEN 'mid' ELSE 'low' END AS revenue_bucket,
    MAX(units) - MIN(units) AS units_range
FROM store_sales
WHERE revenue BETWEEN 1200 AND 3600
GROUP BY CASE WHEN units >= 33 THEN 'high' WHEN units >= 19 THEN 'mid' ELSE 'low' END, store_id, CASE WHEN revenue >= 4400 THEN 'high' WHEN revenue >= 1100 THEN 'mid' ELSE 'low' END;
