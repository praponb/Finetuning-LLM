SELECT
    CASE WHEN budget >= 3174000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    ROUND(SUM(budget) / NULLIF(COUNT(CASE WHEN location = 'Bangkok' THEN 1 END), 0), 2) AS budget_per_bangkok,
    COUNT(*) AS event_count,
    COUNT(*) AS event_count_2,
    PERCENTILE_APPROX(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list
FROM departments
GROUP BY budget_tier;
