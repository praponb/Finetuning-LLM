WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(dept_name) dept_name_upper
    FROM departments
    WHERE location NOT IN ('Bangkok', 'Singapore')
)
-- Keep one row per key (latest first)
SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC NULLS FIRST, id) AS dedup_rank
FROM filtered
QUALIFY dedup_rank = 1
