SELECT
    CASE WHEN YEAR(shipped_date) < 2024 THEN 'before_2024' WHEN YEAR(shipped_date) = 2024 THEN '2024' ELSE 'after_2024' END shipped_cohort,
    CASE WHEN cost >= 77 THEN 'high' WHEN cost >= 17 THEN 'mid' ELSE 'low' END cost_bucket,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values,
    SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END) delivered_count
FROM shipments
GROUP BY shipped_cohort, cost_bucket
HAVING SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END) > 74
ORDER BY delivered_count DESC, shipped_cohort, cost_bucket
LIMIT 5;
