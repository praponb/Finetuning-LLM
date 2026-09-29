WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY job_title ORDER BY salary) AS salary_dense_rank,
        FIRST_VALUE(level, TRUE) OVER (PARTITION BY job_title, level ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) AS salary_share,
        AVG(salary) OVER w_ordered AS salary_avg_5_prior
    FROM employees
    WINDOW w_ordered AS (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC NULLS LAST, id) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ADD_MONTHS(hire_date, 12) AS hire_date_plus_months
    FROM latest
    WHERE hire_date >= TO_DATE('2024-03-25')
),
joined AS (
    -- Attach departments attributes
    SELECT
        a.*,
        d.dept_name,
        d.budget
    FROM filtered a
    INNER JOIN departments d
        ON a.dept_id = d.id
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        a.*,
        b.budget AS department_budget,
        b.dept_name AS department_dept_name
    FROM joined a
    LEFT JOIN departments b
        ON a.dept_id = b.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY salary, id) AS rn
        FROM with_dims
    ) ranked
    WHERE rn = 1
),
matched_keys AS (
    -- Set intersection on keys
    SELECT dept_id
    FROM deduped
    INTERSECT ALL
    SELECT dept_id
    FROM employees
    WHERE dept_id = 21
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN matched_keys k ON s.dept_id = k.dept_id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', hire_date_plus_months) AS week_start,
    dept_id,
    COUNT(*) FILTER (WHERE dept_name = 'Marketing') AS marketing_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(SUM(salary_share) / NULLIF(COUNT(CASE WHEN dept_name = 'Sales' THEN 1 END), 0), 2) AS salary_share_per_sales,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE department_dept_name = 'Sales') AS sales_rows,
    MAX(salary) FILTER (WHERE job_title = 'Analyst') AS analyst_max_salary
FROM kept_rows
GROUP BY CUBE (DATE_TRUNC('WEEK', hire_date_plus_months), dept_id)
HAVING distinct_ids > 1
ORDER BY analyst_max_salary DESC NULLS LAST;
