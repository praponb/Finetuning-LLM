WITH cleaned AS (
    SELECT
        id,
        full_name,
        dept_id,
        job_title,
        hire_date,
        is_remote,
        level,
        CASE WHEN job_title = 'Analyst' THEN 1 ELSE 0 END is_analyst,
        COALESCE(job_title, 'NONE') resolved_job_title
    FROM employees
    WHERE hire_date > DATE '2023-06-18'
      AND job_title IN ('Manager', 'Intern', 'Analyst')
),
joined AS (
    SELECT
        src.*,
        b.dept_name,
        b.budget,
        b.location
    FROM cleaned src
    INNER JOIN departments b
        ON src.dept_id = b.id
    WHERE b.location NOT IN ('Singapore', 'Chiang Mai')
),
valid AS (
    SELECT
        id,
        full_name,
        dept_id,
        job_title,
        hire_date,
        is_remote,
        level,
        is_analyst,
        resolved_job_title,
        dept_name,
        budget,
        location,
        IF(is_remote, 'Yes', 'No') is_remote_label,
        UPPER(location) location_upper,
        COALESCE(level, 'N/A') level_filled
    FROM joined
    WHERE hire_date >= TO_DATE('2023-02-05')
),
cleaned_employees AS (
    SELECT
        id,
        full_name,
        dept_id,
        job_title,
        hire_date,
        is_analyst,
        resolved_job_title,
        dept_name,
        location,
        is_remote_label,
        level_filled,
        DATE_FORMAT(hire_date, 'yyyy-MM-dd') hire_date_label
    FROM valid
    WHERE hire_date > DATE '2023-03-21'
),
ranked AS (
    SELECT *
    FROM cleaned_employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY is_analyst DESC, id ASC) = 1
),
totals AS (
    SELECT
        CASE WHEN is_analyst >= 64 THEN 'HIGH' ELSE 'LOW' END AS is_analyst_tier,
        COUNT(DISTINCT dept_id) AS distinct_dept_ids,
        COUNT(*) AS num_rows
    FROM ranked
    GROUP BY CASE WHEN is_analyst >= 64 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    is_analyst_tier,
    distinct_dept_ids,
    num_rows
FROM totals;
