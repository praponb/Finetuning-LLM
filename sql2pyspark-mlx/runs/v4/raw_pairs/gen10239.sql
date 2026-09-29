SELECT CASE WHEN units >= 40 THEN 'high' WHEN units >= 6 THEN 'mid' ELSE 'low' END AS units_bucket, MIN(revenue) AS min_revenue FROM store_sales GROUP BY units_bucket HAVING MIN(revenue) < 2400;
