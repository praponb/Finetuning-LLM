WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(location, 1, 4) AS location_prefix,
        COALESCE(location, 'UNKNOWN') AS resolved_location
    FROM departments
    WHERE location IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        budget,
        AVG(budget) OVER w_ordered AS budget_avg_3_prior,
        ROW_NUMBER() OVER (ORDER BY budget DESC, id) AS row_seq
    FROM valid
    WINDOW w_ordered AS (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.job_title,
        top1.hire_date
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.job_title, c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date >= s.hire_date
        )
)
SELECT
    id,
    budget,
    row_seq,
    job_title,
    hire_date
FROM kept
ORDER BY hire_date DESC;
