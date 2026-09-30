WITH cleaned AS (
    SELECT *, CONCAT_WS('/', level, full_name) AS level_full_name_key
    FROM employees
    WHERE job_title IS NOT NULL
),
enriched AS (
    SELECT
        *,
        RANK() OVER (ORDER BY salary) AS salary_rank,
        FIRST_VALUE(level) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level
    FROM cleaned
),
remaining_keys AS (
    SELECT manager_id, id
    FROM enriched
    EXCEPT
    SELECT manager_id, id
    FROM employees
    WHERE level = 'L5'
),
kept_rows AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.manager_id = k.manager_id AND s.id = k.id
),
joined AS (
    SELECT
        s.email,
        s.dept_id,
        s.manager_id,
        s.job_title,
        s.is_remote,
        s.first_known_level,
        p.budget,
        p.dept_name,
        p.location
    FROM kept_rows s
    LEFT JOIN departments p
        ON s.dept_id = p.id
),
cleaned_employees AS (
    SELECT
        *,
        COALESCE(location, 'N/A') AS location_filled,
        INITCAP(TRIM(dept_name)) AS dept_name_title,
        COALESCE(job_title, 'UNKNOWN') AS resolved_job_title
    FROM joined
    WHERE manager_id IS NOT NULL
),
filtered AS (
    SELECT *, CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label
    FROM cleaned_employees
    WHERE budget < 3258000
)
SELECT
    manager_id,
    dept_name,
    CASE WHEN budget >= 3523000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    COUNT(*) FILTER (WHERE location = 'New York') AS new_york_rows,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
FROM filtered
GROUP BY 1, 2, 3
HAVING COUNT(*) FILTER (WHERE location = 'New York') >= 1;
