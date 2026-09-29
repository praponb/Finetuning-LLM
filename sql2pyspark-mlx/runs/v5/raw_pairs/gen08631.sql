WITH valid AS (
    SELECT *, COALESCE(location, 'Unknown') AS location_filled
    FROM departments
    WHERE location IN ('Berlin', 'Bangkok', 'Chiang Mai')
),
with_last AS (
    SELECT
        s.id,
        s.dept_name,
        s.location,
        s.budget,
        x.employee_job_title,
        x.employee_hire_date
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title, c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    SELECT
        *,
        LAG(budget) OVER w_running AS prev_budget,
        ROW_NUMBER() OVER w_running AS event_seq,
        DENSE_RANK() OVER w_grp AS budget_dense_rank
    FROM with_last
    WINDOW w_running AS (PARTITION BY dept_name ORDER BY employee_hire_date ASC, id ASC), w_grp AS (PARTITION BY dept_name ORDER BY budget ASC)
),
latest AS (
    SELECT
        id,
        location,
        employee_hire_date,
        prev_budget,
        event_seq,
        budget_dense_rank
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY event_seq ASC NULLS LAST, id ASC) AS dedup_rank
        FROM with_windows
    ) x
    WHERE dedup_rank = 1
),
eligible AS (
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date < s.employee_hire_date
        )
),
latest_departments AS (
    SELECT *, RANK() OVER (PARTITION BY location ORDER BY prev_budget ASC) AS row_num
    FROM eligible
    QUALIFY row_num = 1
),
grouped AS (
    SELECT
        CASE WHEN prev_budget >= 4236000 THEN 'HIGH' ELSE 'LOW' END AS prev_budget_tier,
        SIZE(COLLECT_SET(location)) AS num_locations,
        APPROX_PERCENTILE(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles,
        GROUPING(CASE WHEN prev_budget >= 4236000 THEN 'HIGH' ELSE 'LOW' END) AS gid
    FROM latest_departments
    GROUP BY CUBE (CASE WHEN prev_budget >= 4236000 THEN 'HIGH' ELSE 'LOW' END)
)
SELECT num_locations, event_seq_quartiles
FROM grouped;
