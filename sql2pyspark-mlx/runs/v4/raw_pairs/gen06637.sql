WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(level) clean_level
    FROM employees
    WHERE is_remote = TRUE
)
-- Ranking and gap features
SELECT
    *,
    ROW_NUMBER() OVER w_part event_seq,
    SUM(salary) OVER w_grp running_salary
FROM cleaned
WINDOW w_part AS (ORDER BY hire_date ASC, id ASC), w_grp AS (ORDER BY hire_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
