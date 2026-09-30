WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_id,
        job_title,
        hire_date,
        is_remote,
        FLOOR(salary) salary_whole,
        COALESCE(job_title, 'N/A') job_title_filled
    FROM employees
    WHERE hire_date >= TO_DATE('2024-03-02')
      AND UPPER(TRIM(full_name)) = 'JACK TAYLOR'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_id,
        job_title,
        hire_date,
        salary_whole,
        job_title_filled,
        IF(is_remote, 'Yes', 'No') is_remote_label,
        COALESCE(job_title, 'N/A') job_title_clean
    FROM valid
    WHERE job_title IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_id,
        job_title,
        hire_date,
        salary_whole,
        job_title_filled,
        is_remote_label,
        job_title_clean,
        FIRST_VALUE(job_title, TRUE) OVER w_running first_known_job_title,
        LEAD(salary_whole, 1, 0) OVER w_running_2 next_salary_whole,
        LEAD(salary_whole, 1) OVER (PARTITION BY job_title ORDER BY hire_date, id) next_salary_whole_2
    FROM cleaned
    WINDOW w_running AS (PARTITION BY dept_id ORDER BY salary_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running_2 AS (PARTITION BY job_title ORDER BY salary_whole, id)
),
enriched AS (
    -- Attach departments attributes
    SELECT
        a.*,
        d.location,
        d.dept_name,
        d.budget
    FROM scored a
    JOIN departments d
        ON a.dept_id = d.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', hire_date) AS week_start,
    CASE WHEN budget >= 3366000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    APPROX_PERCENTILE(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list,
    COUNT(*) FILTER (WHERE location = 'London') AS london_rows
FROM enriched
GROUP BY 1, 2
ORDER BY budget_tier DESC NULLS LAST;
