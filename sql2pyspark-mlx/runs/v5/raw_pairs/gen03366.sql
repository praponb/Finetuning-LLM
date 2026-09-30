WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN location = 'Singapore' THEN 1 ELSE 0 END is_singapore
    FROM departments
    WHERE budget IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(is_singapore) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_is_singapore,
        DENSE_RANK() OVER w_grp is_singapore_dense_rank,
        LAG(budget, 2) OVER (PARTITION BY location ORDER BY is_singapore, id) prev_budget
    FROM valid
    WINDOW w_grp AS (PARTITION BY dept_name ORDER BY is_singapore DESC)
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.location,
        s.is_singapore,
        s.is_singapore_dense_rank,
        x.job_title,
        x.salary
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.job_title, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, dept_name
    FROM with_latest
    EXCEPT
    SELECT id, dept_name
    FROM departments
    WHERE location = 'Berlin'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest s
    JOIN kept_keys k ON s.id = k.id AND s.dept_name = k.dept_name
)
-- Multi-level aggregation
SELECT
    CASE WHEN salary >= 150000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
    location,
    ROUND(SUM(is_singapore_dense_rank) / NULLIF(COUNT(CASE WHEN location = 'Singapore' THEN 1 END), 0), 2) AS is_singapore_dense_rank_per_singapore,
    COUNT(*) FILTER (WHERE location = 'Chiang Mai') AS chiang_mai_rows,
    COUNT(*) FILTER (WHERE dept_name = 'HR') AS hr_rows,
    PERCENTILE_APPROX(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles,
    COUNT(*) FILTER (WHERE dept_name = 'Legal') AS legal_rows,
    GROUPING(CASE WHEN salary >= 150000 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_id
FROM verified
GROUP BY GROUPING SETS ((CASE WHEN salary >= 150000 THEN 'PREMIUM' ELSE 'STANDARD' END, location), (CASE WHEN salary >= 150000 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
HAVING is_singapore_dense_rank_per_singapore > 0
ORDER BY salary_tier DESC NULLS LAST
