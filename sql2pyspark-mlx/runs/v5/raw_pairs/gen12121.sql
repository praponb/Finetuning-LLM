WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(dept_name, 'unassigned') AS dept_name_filled
    FROM departments
    WHERE dept_name NOT IN ('Finance', 'Marketing')
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY budget DESC, id) AS event_seq,
        PERCENT_RANK() OVER (PARTITION BY location ORDER BY budget NULLS FIRST) AS budget_percent_rank
    FROM filtered
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY location ORDER BY budget_percent_rank DESC) AS latest_rank
        FROM scored
    ) ranked
    WHERE latest_rank = 1
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget_percent_rank,
        top1.employee_hire_date
    FROM latest s
    JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT id
FROM with_last
ORDER BY id DESC;
