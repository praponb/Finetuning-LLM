WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN budget >= 4153000 THEN 'High' WHEN budget >= 876000 THEN 'Medium' ELSE 'Low' END AS budget_level,
        CONCAT_WS('-', dept_name, location) AS dept_name_location_key
    FROM departments
    WHERE dept_name IS NOT NULL
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.employee_hire_date,
        latest.employee_job_title
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date, c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(budget) OVER w_ordered AS next_budget,
        LAG(budget) OVER w_ordered AS prev_budget,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        MIN(budget) OVER w_ordered_2 AS dept_name_min_budget
    FROM with_latest
    WINDOW w_ordered AS (PARTITION BY location ORDER BY budget ASC, id ASC), w_ordered_2 AS (PARTITION BY dept_name)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        location,
        budget_level,
        employee_hire_date,
        row_seq,
        dept_name_min_budget,
        LAG(budget) OVER (PARTITION BY dept_name, location ORDER BY row_seq ASC, id ASC) AS prev_budget_2,
        LEAD(prev_budget) OVER (PARTITION BY dept_name, location ORDER BY row_seq ASC, id ASC) AS next_prev_budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY row_seq ASC, id ASC) AS row_seq_2
    FROM with_windows
),
remaining_keys AS (
    -- Set difference on keys
    SELECT location
    FROM enriched
    EXCEPT
    SELECT location
    FROM departments
    WHERE location IN ('Tokyo', 'Berlin')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.location = k.location
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.is_remote
        )
),
enriched_departments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(dept_name_min_budget) OVER (PARTITION BY location) AS location_stddev_samp_dept_name_min_budget,
        MIN(next_prev_budget) OVER (PARTITION BY dept_name) AS dept_name_min_next_prev_budget,
        ROUND(row_seq_2 / SUM(row_seq_2) OVER (PARTITION BY location), 4) AS row_seq_2_share
    FROM eligible
)
SELECT
    id,
    dept_name,
    budget_level,
    employee_hire_date,
    dept_name_min_budget,
    prev_budget_2,
    next_prev_budget,
    row_seq_2,
    location_stddev_samp_dept_name_min_budget,
    CASE WHEN dept_name RLIKE '^(Finance|Sales)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS dept_name_zone
FROM enriched_departments
ORDER BY dept_name_zone DESC NULLS LAST
