WITH with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        s.budget,
        x.hire_date
    FROM departments s
    JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS row_seq,
        RANK() OVER (PARTITION BY location ORDER BY budget) AS budget_rank,
        SUM(budget) OVER w_running AS cumulative_budget
    FROM with_latest
    WINDOW w_running AS (PARTITION BY location ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location ORDER BY hire_date NULLS FIRST, id) = 1
),
scored_departments AS (
    -- Ranking and gap features
    SELECT
        id,
        budget,
        hire_date,
        row_seq,
        budget_rank,
        LAG(cumulative_budget, 1) OVER (PARTITION BY location ORDER BY cumulative_budget, id) AS prev_cumulative_budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share_2,
        COUNT(budget_share) OVER (PARTITION BY location) AS location_count_budget_share,
        AVG(cumulative_budget) OVER (ORDER BY row_seq, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS cumulative_budget_avg_3_prior,
        SUM(budget) OVER (PARTITION BY location ORDER BY budget_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget
    FROM latest
)
SELECT
    id,
    budget,
    hire_date,
    row_seq,
    prev_cumulative_budget,
    budget_share_2,
    location_count_budget_share,
    cumulative_budget_avg_3_prior,
    running_budget
FROM scored_departments
ORDER BY cumulative_budget_avg_3_prior DESC;
