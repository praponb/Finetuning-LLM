SELECT
    CASE WHEN budget >= 4108000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
    COUNT(DISTINCT dept_name) AS distinct_dept_names,
    STDDEV_SAMP(budget) AS budget_stddev,
    COUNT(*) AS num_rows,
    STDDEV_SAMP(budget) AS budget_stddev_2,
    SIZE(COLLECT_SET(location)) AS num_locations
FROM departments
GROUP BY CASE WHEN budget >= 4108000 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY num_rows DESC;
