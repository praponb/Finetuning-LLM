WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(location) IGNORE NULLS OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_location,
        RANK() OVER (ORDER BY budget NULLS LAST) AS budget_rank,
        NTILE(10) OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS budget_bucket
    FROM departments
)
-- Grouped metrics
SELECT
    CASE WHEN budget_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_rank_tier,
    COUNT(*) AS row_count,
    COUNT(CASE WHEN dept_name = 'Sales' THEN 1 END) AS sales_count,
    PERCENTILE_APPROX(budget_bucket, ARRAY(0.25, 0.5, 0.75)) AS budget_bucket_quartiles,
    MIN(budget) AS min_budget,
    COUNT(*) AS row_count_2,
    MIN(budget_rank) AS min_budget_rank
FROM metrics
GROUP BY CASE WHEN budget_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END
