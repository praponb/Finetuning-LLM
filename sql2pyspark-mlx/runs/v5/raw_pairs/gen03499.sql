SELECT units_bucket, max_units
FROM (
    SELECT CASE WHEN units >= 44 THEN 'high' WHEN units >= 14 THEN 'mid' ELSE 'low' END AS units_bucket, MAX(units) AS max_units
    FROM store_sales
    GROUP BY CASE WHEN units >= 44 THEN 'high' WHEN units >= 14 THEN 'mid' ELSE 'low' END
) AS t
WHERE max_units > 11
