WITH with_last AS (
    SELECT
        s.id,
        s.dept_name,
        s.location,
        s.budget,
        x.employee_salary
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    SELECT
        *,
        FIRST_VALUE(budget, TRUE) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_budget,
        AVG(budget) OVER (ORDER BY employee_salary, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS budget_avg_5_prior
    FROM with_last
),
ranked AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        employee_salary,
        first_known_budget,
        budget_avg_5_prior
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget DESC NULLS FIRST, id) AS latest_rank
        FROM scored
    ) x
    WHERE latest_rank = 1
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC NULLS FIRST, id) AS rank_in_group
    FROM ranked
    QUALIFY rank_in_group = 1
)
SELECT
    dept_name,
    SUM(first_known_budget) AS total_first_known_budget,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS row_count,
    MAX(budget_avg_5_prior) AS max_budget_avg_5_prior
FROM latest
GROUP BY dept_name
ORDER BY total_first_known_budget NULLS LAST;
