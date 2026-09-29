WITH scored AS (
    SELECT
        *,
        LEAD(budget, 1) OVER (ORDER BY budget, id) AS next_budget,
        NTILE(3) OVER (ORDER BY budget DESC, id) AS budget_bucket,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS seq_no,
        SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget,
        ROW_NUMBER() OVER w_ordered AS event_seq
    FROM departments
    WINDOW w_ordered AS (PARTITION BY dept_name ORDER BY budget, id)
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY event_seq DESC NULLS FIRST, id) AS row_num
    FROM scored
    QUALIFY row_num = 1
),
filtered AS (
    SELECT *, CASE WHEN event_seq >= 7 THEN 'High' WHEN event_seq >= 3 THEN 'Medium' ELSE 'Low' END AS event_seq_tier
    FROM deduped
    WHERE dept_name IN ('Marketing', 'Sales')
),
eligible AS (
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.is_remote
        )
),
kept_keys AS (
    SELECT location, dept_name
    FROM eligible
    INTERSECT
    SELECT location, dept_name
    FROM departments
    WHERE dept_name IS NULL
),
reconciled AS (
    SELECT s.*
    FROM eligible s
    INNER JOIN kept_keys k ON s.location = k.location AND s.dept_name = k.dept_name
)
SELECT
    location,
    budget,
    next_budget,
    budget_bucket,
    seq_no,
    cumulative_budget,
    event_seq,
    row_num
FROM reconciled;
