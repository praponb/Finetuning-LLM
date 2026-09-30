WITH latest AS (
    SELECT *
    FROM departments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget DESC, id) = 1
),
ranked AS (
    SELECT *
    FROM latest
    QUALIFY RANK() OVER (PARTITION BY location ORDER BY budget DESC NULLS LAST) = 1
),
not_flagged AS (
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.salary BETWEEN 118000 AND 143000
        )
),
valid AS (
    SELECT
        *,
        ROUND(budget * 1.1, 2) budget_scaled,
        CASE WHEN budget >= 4180000 THEN 'High' WHEN budget >= 1427000 THEN 'Medium' ELSE 'Low' END budget_tier
    FROM not_flagged
    WHERE location <> 'Chiang Mai'
),
deduped AS (
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget_scaled DESC, id) = 1
),
scored AS (
    SELECT
        id,
        dept_name,
        budget_scaled,
        budget_tier,
        FIRST_VALUE(dept_name, TRUE) OVER (ORDER BY budget_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_dept_name,
        COUNT(budget) OVER (PARTITION BY dept_name, location) dept_name_count_budget,
        ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget, id) event_seq,
        LAST_VALUE(budget, TRUE) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_budget
    FROM deduped
),
base AS (
    SELECT *, event_seq - dept_name_count_budget event_seq_minus_dept_name_count_budget
    FROM scored
    WHERE dept_name_count_budget <> 8
)
SELECT
    dept_name,
    SUM(dept_name_count_budget) FILTER (WHERE dept_name = 'Marketing') AS marketing_total_dept_name_count_budget,
    COUNT(CASE WHEN dept_name = 'Finance' THEN 1 END) AS finance_count,
    COUNT(*) FILTER (WHERE dept_name = 'Finance') AS finance_rows,
    COUNT(*) AS cnt,
    ROUND(AVG(budget_scaled), 2) AS avg_budget_scaled,
    SUM(CASE WHEN dept_name = 'Sales' THEN dept_name_count_budget ELSE 0 END) AS sales_dept_name_count_budget
FROM base
GROUP BY dept_name
HAVING finance_rows > 1
ORDER BY sales_dept_name_count_budget DESC NULLS FIRST
