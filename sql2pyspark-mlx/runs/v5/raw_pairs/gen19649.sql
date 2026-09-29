WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        status,
        INITCAP(TRIM(plan)) AS plan_title
    FROM subscriptions
    WHERE status = 'ACTIVE'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        start_date,
        status,
        plan_title,
        UPPER(plan) AS plan_upper
    FROM filtered
    WHERE start_date >= DATE '2023-06-16'
      AND customer_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, plan ORDER BY start_date, id) AS event_seq
    FROM valid
    WHERE start_date BETWEEN '2023-12-01' AND '2024-08-14'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        status,
        plan_upper,
        event_seq,
        CONCAT_WS('-', status, plan) AS status_plan_key
    FROM with_windows
    WHERE start_date >= TO_DATE('2023-05-06')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        plan_upper,
        event_seq,
        status_plan_key,
        LEAD(event_seq) OVER (PARTITION BY plan ORDER BY event_seq, id) AS next_event_seq,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY event_seq, id) AS seq_no,
        AVG(event_seq) OVER (ORDER BY event_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS event_seq_avg_3
    FROM base
)
SELECT
    id,
    plan,
    plan_upper,
    event_seq,
    seq_no,
    event_seq_avg_3,
    CASE WHEN plan RLIKE '^(basic|enterprise)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS plan_zone
FROM enriched
ORDER BY event_seq_avg_3 DESC NULLS LAST, plan DESC
