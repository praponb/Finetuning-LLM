WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(salary AS INT) AS salary_int,
        COALESCE(job_title, 'N/A') AS job_title_clean
    FROM employees
    WHERE hire_date > DATE '2023-07-08'
      AND job_title IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date DESC, id) AS dedup_rank
        FROM valid
    ) x
    WHERE dedup_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(salary_int) OVER (PARTITION BY level, manager_id ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_salary_int,
        MAX(salary_int) OVER (PARTITION BY level, manager_id ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_salary_int,
        MIN(salary_int) OVER (PARTITION BY level, manager_id ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_salary_int
    FROM latest
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        salary_int,
        job_title_clean,
        running_max_salary_int,
        LAST_VALUE(running_max_salary_int) IGNORE NULLS OVER (ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_max_salary_int,
        LEAD(running_avg_salary_int, 1) OVER (PARTITION BY level ORDER BY running_avg_salary_int, id) AS next_running_avg_salary_int
    FROM metrics
),
enriched_employees AS (
    -- Attach departments attributes
    SELECT
        e.id,
        e.dept_id,
        e.salary,
        e.hire_date,
        e.is_remote,
        e.job_title_clean,
        e.running_max_salary_int,
        e.next_running_avg_salary_int,
        b.budget
    FROM enriched e
    LEFT JOIN departments b
        ON e.dept_id = b.id
    WHERE b.budget BETWEEN 1433000 AND 3318000
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        dept_id,
        salary,
        hire_date,
        is_remote,
        job_title_clean
    FROM enriched_employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY running_max_salary_int DESC, id) = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, dept_id
    FROM deduped
    INTERSECT ALL
    SELECT id, dept_id
    FROM employees
    WHERE level = 'L5'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.dept_id = k.dept_id
)
-- Join a dimension and aggregate
SELECT
    p.dept_name,
    COUNT(*) AS num_rows,
    MIN(a.salary) AS min_salary
FROM reconciled a
LEFT JOIN departments p ON a.dept_id = p.id
GROUP BY ROLLUP (p.dept_name);
