WITH base AS (
    SELECT *, ROUND(salary * 0.9, 2) AS adjusted_salary
    FROM employees
    WHERE dept_id IN (7, 18, 27)
)
SELECT
    *,
    MIN(adjusted_salary) OVER (PARTITION BY dept_id ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_adjusted_salary,
    AVG(adjusted_salary) OVER (PARTITION BY dept_id ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_adjusted_salary,
    COUNT(adjusted_salary) OVER (PARTITION BY dept_id ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_adjusted_salary
FROM base
