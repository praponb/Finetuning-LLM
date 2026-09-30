WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN budget >= 3895000 THEN 'High' WHEN budget >= 1456000 THEN 'Medium' ELSE 'Low' END AS budget_band
    FROM departments
    WHERE location IS DISTINCT FROM 'Bangkok'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(budget, 1) OVER (PARTITION BY location ORDER BY budget, id) AS prev_budget,
        LEAD(budget, 2) OVER (PARTITION BY location ORDER BY budget, id) AS next_budget
    FROM valid
    WHERE budget IS NOT NULL
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget,
        s.prev_budget,
        s.next_budget,
        latest.job_title,
        latest.level
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.job_title, c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        prev_budget,
        next_budget,
        job_title,
        level,
        SUM(prev_budget) OVER (PARTITION BY level ORDER BY prev_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_budget,
        SUM(budget) OVER w_part AS running_budget,
        PERCENT_RANK() OVER (PARTITION BY job_title, level ORDER BY budget NULLS LAST) AS budget_percent_rank,
        LAG(next_budget, 1) OVER w_hist AS prev_next_budget,
        ROW_NUMBER() OVER (ORDER BY next_budget DESC, id) AS event_seq
    FROM with_last
    WINDOW w_part AS (PARTITION BY level, job_title ORDER BY next_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY level, job_title ORDER BY prev_budget, id)
),
with_last_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.prev_budget,
        s.next_budget,
        s.cumulative_prev_budget,
        s.running_budget,
        s.budget_percent_rank,
        latest.employee_level,
        latest.hire_date
    FROM enriched s
    JOIN LATERAL (
            SELECT c.level AS employee_level, c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched_departments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        cumulative_prev_budget,
        running_budget,
        LAG(prev_budget) OVER w_ordered AS prev_prev_budget,
        LEAD(prev_budget) OVER w_ordered AS next_prev_budget,
        LAG(cumulative_prev_budget) OVER w_ordered AS prev_cumulative_prev_budget,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        ROW_NUMBER() OVER w_running AS row_seq
    FROM with_last_departments
    WINDOW w_ordered AS (PARTITION BY employee_level ORDER BY running_budget, id), w_running AS (PARTITION BY employee_level ORDER BY cumulative_prev_budget, id)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(prev_cumulative_prev_budget) OVER (ORDER BY cumulative_prev_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_cumulative_prev_budget,
        ROW_NUMBER() OVER (ORDER BY running_budget DESC, id) AS event_seq_2
    FROM enriched_departments
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN next_prev_budget >= 3131000 THEN 'HIGH' ELSE 'LOW' END AS next_prev_budget_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS num_rows
    FROM with_windows
    GROUP BY 1
    HAVING COUNT(*) > 1
)
SELECT next_prev_budget_tier, num_rows
FROM totals
ORDER BY next_prev_budget_tier DESC, num_rows
