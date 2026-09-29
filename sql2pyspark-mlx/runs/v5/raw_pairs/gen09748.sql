WITH metrics_long AS (
    SELECT
        id,
        category,
        kpi,
        reading
    FROM (
        SELECT
            id,
            category,
            cost,
            price
        FROM products
    ) src
    UNPIVOT INCLUDE NULLS (
        reading FOR kpi IN (cost, price)
    ) long_rows
)
SELECT
    kpi,
    CASE WHEN reading >= 71 THEN 'LARGE' ELSE 'SMALL' END AS reading_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(CASE WHEN category = 'Fashion' THEN 1 END) AS fashion_count,
    SORT_ARRAY(COLLECT_SET(kpi)) AS kpi_list,
    SUM(CASE WHEN kpi = 'cost' THEN reading ELSE 0 END) AS cost_reading,
    COUNT_IF(kpi = 'cost') AS cost_events,
    COUNT(*) AS num_rows,
    RANK() OVER (PARTITION BY kpi ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_rank
FROM metrics_long
GROUP BY kpi, CASE WHEN reading >= 71 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY num_rows DESC NULLS LAST;
