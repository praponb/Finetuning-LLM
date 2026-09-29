WITH filtered AS (
    SELECT
        *,
        CASE WHEN location IN ('Chiang Mai', 'Singapore') THEN 'Core' ELSE 'Other' END AS location_group,
        LOWER(location) AS location_lower,
        COALESCE(location, 'N/A') AS location_filled
    FROM departments
    WHERE location IS NOT NULL
      AND dept_name IS DISTINCT FROM 'Marketing'
),
enriched AS (
    SELECT
        id,
        location_group,
        location_filled,
        LAG(budget) OVER w_ordered AS prev_budget,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        AVG(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS budget_avg_6
    FROM filtered
    WHERE location IN ('New York', 'Tokyo')
    WINDOW w_ordered AS (PARTITION BY location ORDER BY budget, id)
),
with_windows AS (
    SELECT
        id,
        location_group,
        prev_budget,
        budget_avg_6,
        LAG(event_seq) OVER w_part AS prev_event_seq,
        LEAD(budget_avg_6) OVER w_part AS next_budget_avg_6,
        LAG(budget_avg_6) OVER w_part AS prev_budget_avg_6,
        ROW_NUMBER() OVER w_part AS event_seq_2
    FROM enriched
    WINDOW w_part AS (ORDER BY budget_avg_6, id)
),
with_windows_departments AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY budget_avg_6 DESC, id) AS event_seq,
        NTILE(10) OVER (ORDER BY next_budget_avg_6 DESC, id) AS next_budget_avg_6_bucket,
        AVG(event_seq_2) OVER (ORDER BY prev_budget_avg_6, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS event_seq_2_avg_5_prior,
        AVG(prev_budget) OVER (ORDER BY budget_avg_6, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS prev_budget_avg_3_prior
    FROM with_windows
)
SELECT
    id,
    location_group,
    prev_budget,
    budget_avg_6,
    prev_event_seq,
    next_budget_avg_6,
    event_seq,
    event_seq_2_avg_5_prior
FROM with_windows_departments
ORDER BY prev_budget DESC NULLS LAST
