WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        dept_id,
        salary,
        hire_date,
        FIRST_VALUE(level, TRUE) OVER (PARTITION BY level ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level,
        NTILE(10) OVER (PARTITION BY manager_id ORDER BY salary, id) AS salary_bucket
    FROM employees
    WHERE email LIKE '%@gmail.com'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(salary_bucket) OVER (PARTITION BY dept_id) AS dept_id_count_salary_bucket,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS seq_no,
        NTILE(3) OVER w_hist AS salary_bucket_bucket
    FROM enriched
    WINDOW w_hist AS (ORDER BY salary_bucket DESC, id)
),
joined AS (
    -- Attach departments attributes
    SELECT
        s.id,
        s.full_name,
        s.dept_id,
        s.hire_date,
        s.first_known_level,
        s.seq_no,
        b.dept_name,
        b.location,
        b.budget
    FROM metrics s
    INNER JOIN departments b
        ON s.dept_id = b.id
),
joined_employees AS (
    -- Attach departments attributes
    SELECT
        src.id,
        src.full_name,
        src.dept_id,
        src.budget,
        p.budget AS department_budget,
        p.location AS department_location
    FROM joined src
    LEFT JOIN departments p
        ON src.dept_id = p.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_id,
        budget,
        department_budget,
        department_location,
        NTILE(4) OVER (PARTITION BY dept_id ORDER BY department_budget, id) AS department_budget_bucket,
        ROUND(budget / SUM(budget) OVER (PARTITION BY department_location, dept_id), 4) AS budget_share
    FROM joined_employees
),
scored_employees AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(budget, TRUE) OVER w_grp AS first_known_budget,
        AVG(department_budget_bucket) OVER w_grp_2 AS department_budget_bucket_avg_6
    FROM scored
    WINDOW w_grp AS (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp_2 AS (PARTITION BY dept_id ORDER BY department_budget_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(budget * 1.1, 2) AS budget_scaled,
        CASE WHEN department_budget_bucket_avg_6 >= 9 THEN 'High' WHEN department_budget_bucket_avg_6 >= 3 THEN 'Medium' ELSE 'Low' END AS department_budget_bucket_avg_6_level,
        COALESCE(department_location, 'NONE') AS department_location_filled
    FROM scored_employees
    WHERE department_location IS NOT NULL
      AND department_location IS DISTINCT FROM 'Tokyo'
)
-- Grouped metrics
SELECT
    CASE WHEN department_budget_bucket_avg_6 >= 7 THEN 'HIGH' ELSE 'LOW' END AS department_budget_bucket_avg_6_tier,
    CASE WHEN department_budget >= 3348000 THEN 'PREMIUM' ELSE 'STANDARD' END AS department_budget_tier,
    COUNT(CASE WHEN department_location = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
    APPROX_PERCENTILE(department_budget_bucket_avg_6, ARRAY(0.25, 0.5, 0.75)) AS department_budget_bucket_avg_6_quartiles,
    SORT_ARRAY(COLLECT_SET(department_location)) AS department_location_list,
    GROUPING(CASE WHEN department_budget_bucket_avg_6 >= 7 THEN 'HIGH' ELSE 'LOW' END) AS gid
FROM base
GROUP BY GROUPING SETS ((CASE WHEN department_budget_bucket_avg_6 >= 7 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN department_budget >= 3348000 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN department_budget_bucket_avg_6 >= 7 THEN 'HIGH' ELSE 'LOW' END), ())
ORDER BY chiang_mai_count NULLS LAST
