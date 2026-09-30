WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget DESC, id) AS dedup_rank
    FROM departments
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    budget,
    dedup_rank
FROM ranked
ORDER BY id NULLS FIRST
