WITH matched_keys AS (
    SELECT id, location
    FROM departments
    INTERSECT ALL
    SELECT id, location
    FROM departments
    WHERE location IN ('Singapore', 'Chiang Mai')
),
reconciled AS (
    SELECT s.*
    FROM departments s
    JOIN matched_keys k ON s.id = k.id AND s.location = k.location
),
not_flagged AS (
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.manager_id IS NULL
        )
),
scored AS (
    SELECT
        id,
        dept_name,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share,
        RANK() OVER w_part AS budget_rank,
        STDDEV_SAMP(budget) OVER (PARTITION BY dept_name, location) AS dept_name_stddev_samp_budget
    FROM not_flagged
    WINDOW w_part AS (PARTITION BY location, dept_name ORDER BY budget NULLS FIRST)
),
filtered AS (
    SELECT
        *,
        COALESCE(dept_name, 'unassigned') AS dept_name_filled,
        GREATEST(budget_rank, budget_share) AS greatest_budget_rank_budget_share
    FROM scored
    WHERE dept_name IS NOT NULL
)
SELECT
    id,
    budget_share,
    budget_rank,
    dept_name_stddev_samp_budget
FROM filtered
ORDER BY dept_name_stddev_samp_budget
