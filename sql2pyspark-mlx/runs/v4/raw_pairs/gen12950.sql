WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('YEAR', hire_date) AS hire_date_period,
        COALESCE(level, 'DIRECT') AS level_clean
    FROM employees
    WHERE hire_date >= DATE '2023-10-08'
)
-- QUALIFY-style window filter
SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY level ORDER BY hire_date DESC) AS rn
    FROM base
) ranked
WHERE rn = 1;
