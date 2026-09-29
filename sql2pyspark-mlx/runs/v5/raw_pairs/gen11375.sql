SELECT
    CASE WHEN dept_name IN ('Legal', 'Sales') THEN 'tier_1' ELSE 'other' END AS dept_name_group,
    SUM(budget) AS total_budget,
    COUNT(*) AS cnt,
    ROUND(SUM(budget) * 100.0 / SUM(SUM(budget)) OVER (), 2) AS pct_of_total
FROM departments
GROUP BY CASE WHEN dept_name IN ('Legal', 'Sales') THEN 'tier_1' ELSE 'other' END
ORDER BY pct_of_total;
