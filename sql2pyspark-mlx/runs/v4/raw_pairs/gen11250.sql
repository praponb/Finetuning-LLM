WITH valid AS (
    SELECT *, ROUND(budget * 1.15, 2) adjusted_budget
    FROM departments
    WHERE dept_name IS NOT NULL
),
scored AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        adjusted_budget,
        SUM(adjusted_budget) OVER w_hist cumulative_adjusted_budget,
        NTILE(5) OVER (PARTITION BY dept_name ORDER BY adjusted_budget, id) adjusted_budget_bucket
    FROM valid
    WINDOW w_hist AS (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    SELECT
        *,
        COALESCE(budget, 0) budget_filled,
        COALESCE(dept_name, 'OTHER') dept_name_filled
    FROM scored
    WHERE adjusted_budget_bucket <> 6
),
with_last AS (
    SELECT s.*, latest.employee_salary
    FROM base s
    JOIN LATERAL (
            SELECT c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY adjusted_budget, id) AS rank_in_group
    FROM with_last
    QUALIFY rank_in_group = 1
),
eligible AS (
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND NOT ch.is_remote
        )
)
SELECT
    id,
    dept_name,
    location,
    budget,
    adjusted_budget,
    cumulative_adjusted_budget,
    adjusted_budget_bucket,
    budget_filled,
    dept_name_filled,
    CASE WHEN location RLIKE '^(New York|London)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS location_zone
FROM eligible
ORDER BY location_zone DESC
