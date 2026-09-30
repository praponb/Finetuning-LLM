WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        hire_date,
        level,
        DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_date_label
    FROM employees
    WHERE hire_date >= DATE '2023-04-14'
      AND manager_id IS NOT NULL
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        src.id,
        src.full_name,
        src.email,
        src.dept_id,
        src.hire_date,
        src.level,
        d.dept_name
    FROM filtered src
    LEFT JOIN departments d
        ON src.dept_id = d.id
    WHERE d.location <> 'Chiang Mai'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY hire_date DESC, id) AS dedup_rank
    FROM with_dims
    QUALIFY dedup_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT *, ROW_NUMBER() OVER (ORDER BY dedup_rank, id) AS event_seq
    FROM ranked
),
joined AS (
    -- Attach departments attributes
    SELECT s.*, b.dept_name AS department_dept_name
    FROM scored s
    LEFT JOIN departments b
        ON s.dept_id = b.id
)
-- Grouped metrics
SELECT
    level,
    dept_id,
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    APPROX_PERCENTILE(event_seq, 0.9) AS event_seq_p90,
    COUNT(*) FILTER (WHERE level = 'L4') AS l4_rows,
    ROUND(SUM(event_seq) / NULLIF(COUNT(CASE WHEN department_dept_name = 'Marketing' THEN 1 END), 0), 2) AS event_seq_per_marketing,
    RANK() OVER (PARTITION BY level ORDER BY COUNT(*) FILTER (WHERE level = 'L4') DESC) AS l4_rows_rank
FROM joined
GROUP BY level, dept_id, DATE_TRUNC('QUARTER', hire_date)
ORDER BY l4_rows DESC;
