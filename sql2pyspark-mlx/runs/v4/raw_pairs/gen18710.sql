SELECT
    product_id,
    store_id,
    MAX(revenue) AS max_revenue,
    MIN(units) AS min_units,
    ROUND(STDDEV(revenue), 2) AS stddev_revenue
FROM store_sales
WHERE revenue > 3400 AND store_id IN (1, 2, 3, 4)
GROUP BY product_id, store_id
ORDER BY 4 DESC
