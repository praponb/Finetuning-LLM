WITH filtered AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        salary,
        hire_date,
        level,
        CONCAT_WS('-', email, job_title) email_job_title_key,
        COALESCE(job_title, 'N/A') job_title_clean
    FROM employees
    WHERE hire_date > TO_DATE('2023-03-28')
),
cleaned AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        hire_date,
        level,
        email_job_title_key,
        DATE_ADD(hire_date, 59) hire_date_plus_59d,
        COALESCE(level, 'N/A') level_clean
    FROM filtered
    WHERE level IS NOT NULL
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date_plus_59d DESC NULLS FIRST, id) AS latest_rank
        FROM cleaned
    ) ranked
    WHERE latest_rank = 1
),
scored AS (
    SELECT
        *,
        FIRST_VALUE(level, TRUE) OVER w_running first_known_level,
        ROW_NUMBER() OVER (PARTITION BY manager_id, dept_id ORDER BY hire_date_plus_59d, id) row_seq
    FROM ranked
    WINDOW w_running AS (PARTITION BY manager_id, dept_id ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    SELECT id, dept_id
    FROM scored
    EXCEPT
    SELECT id, dept_id
    FROM employees
    WHERE level IN ('L3', 'L2', 'L4')
),
kept_rows AS (
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.dept_id = k.dept_id
)
SELECT
    level,
    APPROX_PERCENTILE(row_seq, 0.5) AS row_seq_p50,
    PERCENTILE_APPROX(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles,
    SIZE(COLLECT_SET(level)) AS num_levels,
    PERCENTILE_APPROX(row_seq, 0.5) AS row_seq_p50_2,
    GROUPING(level) AS gid
FROM kept_rows
GROUP BY CUBE (level)
HAVING SIZE(COLLECT_SET(level)) >= 1
ORDER BY row_seq_p50_2 DESC NULLS FIRST;
