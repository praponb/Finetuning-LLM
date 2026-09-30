WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level, manager_id ORDER BY hire_date, id) = 1
)
-- Aggregate with conditional metrics
SELECT
    level,
    ROUND(AVG(salary), 2) AS avg_salary,
    COUNT(DISTINCT manager_id) AS distinct_manager_ids,
    COUNT(CASE WHEN job_title = 'Engineer' THEN 1 END) AS engineer_count
FROM deduped
GROUP BY level
ORDER BY distinct_manager_ids DESC
