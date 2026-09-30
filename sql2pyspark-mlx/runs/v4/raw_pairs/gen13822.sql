SELECT
    status,
    CASE WHEN YEAR(delivered_date) < 2024 THEN 'before_2024' WHEN YEAR(delivered_date) = 2024 THEN '2024' ELSE 'after_2024' END AS delivered_cohort,
    ROUND(STDDEV(cost), 2) AS stddev_cost,
    SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list
FROM shipments
WHERE order_id IN (13, 17, 27, 31)
GROUP BY 1, 2
ORDER BY status DESC;
