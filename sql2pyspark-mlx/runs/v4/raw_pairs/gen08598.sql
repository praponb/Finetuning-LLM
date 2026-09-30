WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(dept_name)) AS dept_name_title,
        CONCAT_WS('/', dept_name, location) AS dept_name_location_key,
        COALESCE(dept_name, 'OTHER') AS dept_name_clean
    FROM departments
    WHERE location IN ('Bangkok', 'Chiang Mai')
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget DESC NULLS FIRST, id) AS dedup_rank
        FROM latest
    ) ranked
    WHERE dedup_rank = 1
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.salary,
        top1.employee_level
    FROM deduped s
    JOIN LATERAL (
            SELECT c.salary, c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN salary >= 138000 THEN 'HIGH' ELSE 'LOW' END AS salary_tier,
    APPROX_PERCENTILE(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles,
    SIZE(COLLECT_SET(employee_level)) AS num_employee_levels,
    COUNT(*) AS cnt
FROM latest_match
GROUP BY CASE WHEN salary >= 138000 THEN 'HIGH' ELSE 'LOW' END
HAVING num_employee_levels >= 1;
