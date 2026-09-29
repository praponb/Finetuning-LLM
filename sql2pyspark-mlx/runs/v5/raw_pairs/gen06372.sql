SELECT
    CASE WHEN units >= 45 THEN 'high' WHEN units >= 13 THEN 'mid' ELSE 'low' END AS units_bucket,
    store_id,
    COUNT(units) AS units_count,
    MAX(sale_date) AS latest_sale_date,
    ROUND(STDDEV(revenue), 2) AS stddev_revenue
FROM store_sales
WHERE units <> 25
GROUP BY CASE WHEN units >= 45 THEN 'high' WHEN units >= 13 THEN 'mid' ELSE 'low' END, store_id
ORDER BY stddev_revenue ASC, units_bucket DESC, store_id ASC
LIMIT 5
