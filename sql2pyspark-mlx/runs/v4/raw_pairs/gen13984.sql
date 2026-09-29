WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        IF(is_remote, 'Yes', 'No') AS is_remote_label,
        COALESCE(job_title, 'UNKNOWN') AS job_title_clean
    FROM employees
    WHERE hire_date > DATE '2023-03-18'
      AND hire_date < '2023-05-01'
),
enriched AS (
    -- Attach departments attributes
    SELECT src.*, b.dept_name
    FROM valid src
    LEFT JOIN departments b
        ON src.dept_id = b.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT dept_id, id
    FROM enriched
    INTERSECT
    SELECT dept_id, id
    FROM employees
    WHERE job_title IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.dept_id = k.dept_id AND s.id = k.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER w_part AS salary_cume_dist,
        MAX(salary) OVER (PARTITION BY dept_id) AS dept_id_max_salary
    FROM kept_rows
    WINDOW w_part AS (PARTITION BY dept_id ORDER BY salary DESC)
),
enriched_employees AS (
    -- Attach departments attributes
    SELECT
        src.*,
        p.location,
        p.budget,
        p.dept_name AS department_dept_name
    FROM with_windows src
    INNER JOIN departments p
        ON src.dept_id = p.id
)
SELECT
    id,
    email,
    dept_id,
    job_title,
    salary,
    is_remote_label,
    job_title_clean,
    dept_name,
    dept_id_max_salary
FROM enriched_employees
ORDER BY id DESC NULLS LAST, job_title DESC;
