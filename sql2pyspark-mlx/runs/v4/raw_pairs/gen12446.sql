WITH base AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        CASE WHEN level IN ('L2', 'L1') THEN 'Priority' ELSE 'Other' END AS level_group,
        COALESCE(level, 'DIRECT') AS level_clean
    FROM employees
    WHERE hire_date > DATE '2023-11-23'
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary, id) AS row_num
    FROM base
    QUALIFY row_num = 1
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_grp AS row_seq,
        AVG(salary) OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS salary_avg_3,
        CUME_DIST() OVER (PARTITION BY level ORDER BY salary DESC) AS salary_cume_dist,
        LAST_VALUE(level) IGNORE NULLS OVER (PARTITION BY job_title, dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
        NTILE(5) OVER (PARTITION BY level ORDER BY salary, id) AS salary_bucket
    FROM deduped
    WINDOW w_grp AS (PARTITION BY job_title, level ORDER BY salary, id)
),
latest AS (
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id) = 1
),
joined AS (
    SELECT
        a.*,
        p.dept_name,
        p.budget,
        p.location
    FROM latest a
    LEFT JOIN departments p
        ON a.dept_id = p.id
),
matched_keys AS (
    SELECT id, dept_id
    FROM joined
    EXCEPT ALL
    SELECT id, dept_id
    FROM employees
    WHERE dept_id IN (11, 19, 33, 34)
),
kept_rows AS (
    SELECT s.*
    FROM joined s
    INNER JOIN matched_keys k ON s.id = k.id AND s.dept_id = k.dept_id
)
SELECT
    CASE WHEN row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    SUM(budget) FILTER (WHERE job_title = 'Manager') AS manager_total_budget,
    APPROX_PERCENTILE(salary_bucket, ARRAY(0.25, 0.5, 0.75)) AS salary_bucket_quartiles,
    SIZE(COLLECT_SET(location)) AS num_locations
FROM kept_rows
GROUP BY CASE WHEN row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY manager_total_budget DESC NULLS FIRST
