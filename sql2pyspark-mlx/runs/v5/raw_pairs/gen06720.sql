WITH with_windows AS (
    SELECT
        *,
        LAG(budget) OVER w_grp AS prev_budget,
        ROW_NUMBER() OVER w_grp AS seq_no,
        CUME_DIST() OVER (PARTITION BY dept_name ORDER BY budget ASC) AS budget_cume_dist
    FROM departments
    WINDOW w_grp AS (PARTITION BY location ORDER BY budget ASC, id ASC)
),
enriched AS (
    SELECT
        id,
        dept_name,
        location,
        prev_budget,
        seq_no,
        AVG(budget) OVER (PARTITION BY dept_name ORDER BY prev_budget ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS budget_avg_3_prior,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share,
        FIRST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY dept_name, location ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_dept_name
    FROM with_windows
),
latest_match AS (
    SELECT
        s.id,
        s.dept_name,
        s.prev_budget,
        s.first_known_dept_name,
        top1.level,
        top1.employee_salary
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.level, c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    SELECT
        *,
        AVG(prev_budget) OVER (PARTITION BY dept_name) AS dept_name_avg_prev_budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY prev_budget ASC, id ASC) AS row_seq
    FROM latest_match
    WHERE level NOT IN ('L5', 'L4')
)
SELECT *
FROM (
    SELECT
        id,
        dept_name,
        employee_salary,
        prev_budget
    FROM scored
) src
UNPIVOT (
    metric_value FOR measure IN (employee_salary AS EMPLOYEE_SALARY, prev_budget AS PREV_BUDGET)
);
