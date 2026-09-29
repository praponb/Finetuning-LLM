WITH filtered AS (
    SELECT *, CONCAT(location, '/', dept_name) AS location_dept_name
    FROM departments
    WHERE location IS NOT NULL
      AND dept_name = 'Finance'
),
deduped AS (
    SELECT
        id,
        location,
        budget
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget DESC, id) AS dedup_rank
        FROM filtered
    ) x
    WHERE dedup_rank = 1
),
base AS (
    SELECT
        *,
        COALESCE(budget, 0) AS budget_filled,
        INITCAP(TRIM(location)) AS location_title,
        COALESCE(location, 'UNKNOWN') AS location_clean
    FROM deduped
    WHERE location IS NOT NULL
      AND location NOT IN ('Tokyo', 'London')
),
valid AS (
    SELECT
        id,
        location,
        budget_filled,
        location_title,
        location_clean,
        budget_filled * budget AS budget_filled_times_budget
    FROM base
    WHERE location NOT IN ('Chiang Mai', 'Berlin')
),
with_last AS (
    SELECT
        s.id,
        s.location_title,
        s.budget_filled_times_budget,
        latest.job_title
    FROM valid s
    JOIN LATERAL (
            SELECT c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    job_title,
    AVG(budget_filled_times_budget) FILTER (WHERE job_title = 'Intern') AS intern_avg_budget_filled_times_budget,
    COUNT(*) AS num_rows,
    COUNT(*) AS cnt,
    MIN(budget_filled_times_budget) AS min_budget_filled_times_budget
FROM with_last
GROUP BY job_title;
