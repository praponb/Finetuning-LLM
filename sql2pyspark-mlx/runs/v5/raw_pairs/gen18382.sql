WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM departments
    QUALIFY RANK() OVER (PARTITION BY location, dept_name ORDER BY budget DESC) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        budget,
        COUNT(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_budget,
        SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_budget,
        AVG(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_budget,
        MAX(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share
    FROM deduped
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.last4_count_budget,
        s.last4_sum_budget,
        latest.job_title,
        latest.employee_salary
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.job_title, c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_last
    INTERSECT
    SELECT id
    FROM departments
    WHERE budget BETWEEN 2296000 AND 2721000
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    INNER JOIN kept_keys k ON s.id = k.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN last4_count_budget >= 18 THEN 'High' WHEN last4_count_budget >= 6 THEN 'Medium' ELSE 'Low' END AS last4_count_budget_tier,
        ROUND(last4_sum_budget * 1.1, 2) AS last4_sum_budget_adjusted,
        COALESCE(job_title, 'OTHER') AS job_title_clean
    FROM reconciled
    WHERE job_title IS NOT NULL
)
-- Grouped metrics
SELECT
    CASE WHEN last4_sum_budget >= 21949000 THEN 'LARGE' ELSE 'SMALL' END AS last4_sum_budget_tier,
    CASE WHEN last4_sum_budget_adjusted >= 71 THEN 'PREMIUM' ELSE 'STANDARD' END AS last4_sum_budget_adjusted_tier,
    job_title,
    COUNT(*) AS row_count,
    ROUND(SUM(last4_count_budget) / NULLIF(COUNT(CASE WHEN job_title = 'Engineer' THEN 1 END), 0), 2) AS last4_count_budget_per_engineer,
    COUNT(CASE WHEN job_title = 'Designer' THEN 1 END) AS designer_count,
    GROUPING(CASE WHEN last4_sum_budget >= 21949000 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_level
FROM base
GROUP BY GROUPING SETS ((CASE WHEN last4_sum_budget >= 21949000 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN last4_sum_budget_adjusted >= 71 THEN 'PREMIUM' ELSE 'STANDARD' END, job_title), (CASE WHEN last4_sum_budget >= 21949000 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN last4_sum_budget_adjusted >= 71 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
HAVING row_count >= 1;
