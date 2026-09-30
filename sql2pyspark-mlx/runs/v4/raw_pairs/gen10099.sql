WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        status,
        DATEDIFF(CURRENT_DATE(), start_date) AS days_since_start_date
    FROM subscriptions
    WHERE customer_id IN (1, 27, 35, 37)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(days_since_start_date, 1) OVER (PARTITION BY status ORDER BY days_since_start_date, id) AS next_days_since_start_date,
        RANK() OVER (PARTITION BY status ORDER BY days_since_start_date NULLS LAST) AS days_since_start_date_rank
    FROM cleaned
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(end_date, 'MMM yyyy') AS end_date_label
    FROM with_windows
    WHERE end_date > TO_DATE('2024-05-12')
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN days_since_start_date >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END AS days_since_start_date_tier,
        AVG(days_since_start_date_rank) AS avg_days_since_start_date_rank,
        COUNT(*) AS num_rows,
        AVG(next_days_since_start_date) FILTER (WHERE status = 'PAUSED') AS paused_avg_next_days_since_start_date
    FROM valid
    GROUP BY 1
)
SELECT days_since_start_date_tier, paused_avg_next_days_since_start_date
FROM stats
ORDER BY paused_avg_next_days_since_start_date;
