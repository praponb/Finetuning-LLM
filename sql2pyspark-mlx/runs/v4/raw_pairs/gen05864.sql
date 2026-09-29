WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM departments
    QUALIFY RANK() OVER (PARTITION BY location ORDER BY budget DESC NULLS FIRST) = 1
)
SELECT
    id,
    dept_name,
    location,
    budget
FROM latest
ORDER BY location DESC NULLS FIRST, budget DESC
