WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(dept_name) AS dept_name_upper,
        COALESCE(location, 'UNKNOWN') AS location_filled
    FROM departments
    WHERE dept_name IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget DESC NULLS FIRST, id) AS dedup_rank
        FROM filtered
    ) ranked
    WHERE dedup_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name,
        budget,
        dept_name_upper,
        location_filled,
        LAG(budget) OVER w_ordered AS prev_budget,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        SUM(budget) OVER w_running AS running_budget
    FROM deduped
    WHERE location IN ('Bangkok', 'Singapore')
    WINDOW w_ordered AS (PARTITION BY location, dept_name ORDER BY dedup_rank, id), w_running AS (PARTITION BY location, dept_name ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget,
        s.dept_name_upper,
        top1.employee_hire_date
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date < s.employee_hire_date
        )
),
latest_match_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget,
        s.dept_name_upper,
        s.employee_hire_date,
        x.salary,
        x.job_title
    FROM kept s
    JOIN LATERAL (
            SELECT c.salary, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        budget,
        dept_name_upper,
        employee_hire_date,
        salary,
        job_title,
        DENSE_RANK() OVER (PARTITION BY job_title ORDER BY salary DESC NULLS LAST) AS salary_dense_rank,
        RANK() OVER (PARTITION BY job_title ORDER BY budget DESC NULLS FIRST) AS budget_rank
    FROM latest_match_departments
)
SELECT
    id,
    budget,
    dept_name_upper,
    employee_hire_date,
    salary,
    job_title,
    salary_dense_rank,
    CASE WHEN job_title RLIKE '^(Analyst|Director)$' THEN 'CORE' WHEN job_title RLIKE '^(Engineer|Manager)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS job_title_zone
FROM scored
ORDER BY job_title_zone NULLS LAST;
