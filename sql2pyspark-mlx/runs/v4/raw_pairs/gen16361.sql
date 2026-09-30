WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        LENGTH(job_title) AS job_title_length,
        COALESCE(job_title, 'NONE') AS job_title_filled
    FROM employees
    WHERE hire_date > TO_DATE('2024-03-06')
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        src.*,
        b.dept_name,
        b.budget,
        b.location
    FROM cleaned src
    LEFT JOIN departments b
        ON src.dept_id = b.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level), 4) AS salary_share,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location, job_title), 4) AS budget_share,
        SUM(budget) OVER (PARTITION BY level, job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget,
        FIRST_VALUE(budget, TRUE) OVER (PARTITION BY job_title, level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_budget,
        LEAD(job_title_length, 1) OVER (PARTITION BY dept_name ORDER BY salary, id) AS next_job_title_length
    FROM with_dims
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        job_title_length,
        job_title_filled,
        dept_name,
        budget,
        location,
        salary_share,
        budget_share,
        cumulative_budget,
        first_known_budget,
        next_job_title_length,
        LAST_VALUE(budget_share, TRUE) OVER (PARTITION BY dept_name, job_title ORDER BY job_title_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_budget_share,
        SUM(budget_share) OVER (ORDER BY budget_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget_share,
        NTILE(10) OVER (PARTITION BY job_title, location ORDER BY budget_share, id) AS budget_share_bucket
    FROM with_windows
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_id,
        salary,
        is_remote,
        job_title_length,
        job_title_filled,
        budget,
        location,
        cumulative_budget,
        first_known_budget,
        next_job_title_length,
        last_known_budget_share,
        cumulative_budget_share,
        budget_share_bucket,
        ROUND(budget_share, 2) AS budget_share_rounded,
        ROUND(salary_share * 1.15, 2) AS salary_share_adjusted
    FROM enriched
    WHERE budget_share <= 0.23
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY first_known_budget, id) AS rn
        FROM filtered
    ) x
    WHERE rn = 1
),
matched_keys AS (
    -- Set intersection on keys
    SELECT dept_id, id
    FROM ranked
    INTERSECT ALL
    SELECT dept_id, id
    FROM employees
    WHERE job_title IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN matched_keys k ON s.dept_id = k.dept_id AND s.id = k.id
)
-- Grouped metrics
SELECT
    CASE WHEN next_job_title_length >= 87 THEN 'LARGE' ELSE 'SMALL' END AS next_job_title_length_tier,
    CASE WHEN last_known_budget_share >= 0.61 THEN 'LARGE' ELSE 'SMALL' END AS last_known_budget_share_tier,
    ROUND(AVG(budget_share_rounded), 2) AS avg_budget_share_rounded,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list,
    ROUND(AVG(job_title_length), 2) AS avg_job_title_length
FROM verified
GROUP BY next_job_title_length_tier, last_known_budget_share_tier
