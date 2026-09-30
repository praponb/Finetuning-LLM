WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        AVG(amount) OVER w_grp AS amount_avg_3_prior,
        AVG(amount) OVER (PARTITION BY order_id, method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_avg_30d,
        LAG(amount, 1) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_amount,
        SUM(amount) OVER w_recent AS running_amount
    FROM payments
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), w_recent AS (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_grp AS seq_no,
        SUM(amount_avg_3_prior) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_avg_3_prior_sum_90d,
        LAST_VALUE(amount) IGNORE NULLS OVER w_ordered AS last_known_amount,
        FIRST_VALUE(running_amount, TRUE) OVER (PARTITION BY status, order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_amount
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY method ORDER BY amount, id), w_ordered AS (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at,
        ROUND(is_new_burst, 1) AS is_new_burst_rounded
    FROM sessions_payments
    WHERE method IS NOT NULL
)
SELECT
    id,
    amount,
    paid_at,
    status,
    amount_avg_30d,
    prev_amount,
    running_amount,
    is_new_burst,
    session_no,
    CASE WHEN status RLIKE '^(FAILED|SUCCESS)$' THEN 'CORE' WHEN status RLIKE '^(REFUNDED)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS status_zone
FROM valid;
