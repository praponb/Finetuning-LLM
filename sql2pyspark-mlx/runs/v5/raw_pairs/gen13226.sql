SELECT
    CASE WHEN revenue >= 4000 THEN 'high' WHEN revenue >= 1300 THEN 'mid' ELSE 'low' END AS revenue_bucket,
    product_id,
    ROUND(STDDEV(units), 2) AS stddev_units,
    MAX(revenue) - MIN(revenue) AS revenue_range
FROM store_sales
WHERE product_id IN (2, 4, 5, 7) AND store_id IN (2, 4, 6) AND product_id = 3
GROUP BY revenue_bucket, product_id
ORDER BY product_id DESC;
