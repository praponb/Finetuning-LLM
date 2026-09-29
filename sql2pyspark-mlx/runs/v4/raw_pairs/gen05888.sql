WITH cleaned AS (
    SELECT
        *,
        LENGTH(email) AS email_length,
        CASE WHEN salary >= 140000 THEN 'High' WHEN salary >= 60000 THEN 'Medium' ELSE 'Low' END AS salary_level,
        COALESCE(level, 'DIRECT') AS resolved_level
    FROM employees
    WHERE hire_date >= DATE '2024-01-23'
      AND manager_id IS NOT NULL
      AND email IS NULL
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level, job_title ORDER BY email_length DESC, id ASC) AS dedup_rank
    FROM cleaned
    QUALIFY dedup_rank = 1
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY dedup_rank DESC, id ASC) AS rank_in_group
    FROM latest
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    full_name,
    email,
    manager_id,
    is_remote,
    level,
    salary_level,
    rank_in_group
FROM ranked;
