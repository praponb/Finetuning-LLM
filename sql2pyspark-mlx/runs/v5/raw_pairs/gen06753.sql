WITH joined AS (
    -- Attach departments attributes
    SELECT
        s.*,
        p.location,
        p.dept_name
    FROM employees s
    INNER JOIN departments p
        ON s.dept_id = p.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary, id) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(salary) OVER w_ordered AS prev_salary,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        ROUND(salary / SUM(salary) OVER (PARTITION BY location, level), 4) AS salary_share
    FROM latest
    WINDOW w_ordered AS (PARTITION BY manager_id ORDER BY salary, id)
),
scored_employees AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(event_seq, 2, 0) OVER (ORDER BY prev_salary, id) AS prev_event_seq,
        LAST_VALUE(level, TRUE) OVER (PARTITION BY level ORDER BY prev_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
        NTILE(3) OVER (PARTITION BY level ORDER BY event_seq, id) AS event_seq_bucket
    FROM scored
    WHERE hire_date <= '2024-01-18'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        email,
        level,
        location,
        dept_name,
        prev_salary,
        event_seq,
        last_known_level,
        event_seq_bucket,
        LEAD(salary) OVER w_ordered AS next_salary,
        LAG(event_seq_bucket) OVER w_ordered AS prev_event_seq_bucket,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        LEAD(event_seq_bucket, 1, 0) OVER (PARTITION BY dept_id ORDER BY event_seq, id) AS next_event_seq_bucket
    FROM scored_employees
    WINDOW w_ordered AS (PARTITION BY level ORDER BY event_seq, id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY event_seq_bucket DESC, id) AS row_seq,
        RANK() OVER (ORDER BY next_salary DESC NULLS LAST) AS next_salary_rank,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY next_event_seq_bucket DESC, id) AS event_seq_2,
        ROW_NUMBER() OVER (ORDER BY prev_event_seq_bucket DESC, id) AS event_seq_3,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY next_salary DESC, id) AS seq_no_2
    FROM metrics
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT level, id
    FROM enriched
    INTERSECT ALL
    SELECT level, id
    FROM employees
    WHERE level = 'L1'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.level = k.level AND s.id = k.id
)
-- Grouped metrics
SELECT
    location,
    level,
    dept_name,
    COUNT(CASE WHEN dept_name = 'Sales' THEN 1 END) AS sales_count,
    SUM(CASE WHEN location = 'London' THEN row_seq ELSE 0 END) AS london_row_seq
FROM verified
GROUP BY location, level, dept_name
HAVING COUNT(CASE WHEN dept_name = 'Sales' THEN 1 END) > 1
ORDER BY london_row_seq DESC;
