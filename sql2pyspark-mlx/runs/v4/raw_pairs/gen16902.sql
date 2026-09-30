SELECT
    CASE WHEN cost >= 76 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    DATE_TRUNC('YEAR', shipped_date) AS year_start,
    COUNT(*) AS row_count,
    SIZE(COLLECT_SET(status)) AS num_status,
    GROUPING(CASE WHEN cost >= 76 THEN 'LARGE' ELSE 'SMALL' END) AS gid
FROM shipments
GROUP BY ROLLUP (CASE WHEN cost >= 76 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', shipped_date))
HAVING num_status >= 1
ORDER BY cost_tier DESC;
