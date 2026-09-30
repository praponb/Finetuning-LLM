SELECT *
FROM (
    SELECT CASE WHEN units >= 39 THEN 'high' WHEN units >= 7 THEN 'mid' ELSE 'low' END units_bucket, MIN(revenue) min_revenue
    FROM store_sales
    GROUP BY CASE WHEN units >= 39 THEN 'high' WHEN units >= 7 THEN 'mid' ELSE 'low' END
) sub
WHERE min_revenue >= 3600
ORDER BY units_bucket
