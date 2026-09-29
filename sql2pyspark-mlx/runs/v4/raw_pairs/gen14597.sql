WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        REGEXP_EXTRACT(email, '@(.+)$', 1) AS email_domain
    FROM employees
    WHERE hire_date > '2023-03-01'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) AS latest_rank
        FROM valid
    ) ranked
    WHERE latest_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(salary, 1) OVER w_recent AS next_salary,
        RANK() OVER (ORDER BY salary DESC) AS salary_rank
    FROM deduped
    WINDOW w_recent AS (PARTITION BY job_title, level ORDER BY latest_rank, id)
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        s.*,
        b.dept_name,
        b.budget,
        b.location
    FROM enriched s
    LEFT JOIN departments b
        ON s.dept_id = b.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        email_domain,
        latest_rank,
        next_salary,
        salary_rank,
        dept_name,
        budget,
        location,
        NTILE(4) OVER (ORDER BY salary DESC, id) AS salary_bucket,
        FIRST_VALUE(budget, TRUE) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY job_title), 4) AS budget_share,
        SUM(budget) OVER (ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget
    FROM with_dims
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        location,
        next_salary,
        budget_share
    FROM with_windows
) src
UNPIVOT (
    val FOR measure IN (next_salary, budget_share AS BUDGET_SHARE_VAL)
);
