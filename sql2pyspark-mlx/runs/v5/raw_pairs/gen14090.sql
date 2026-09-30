WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        DATE_TRUNC('MONTH', paid_at) AS paid_at_period,
        DATE_TRUNC('year', paid_at) AS paid_at_period_2
    FROM payments
    WHERE status = 'REFUNDED'
      AND paid_at > TIMESTAMP '2023-10-25 22:00:00'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        status,
        paid_at_period_2,
        LEAD(amount) OVER (PARTITION BY order_id, status ORDER BY paid_at_period, id) AS next_amount,
        ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY paid_at_period, id) AS row_seq,
        SUM(amount) OVER (PARTITION BY order_id, status) AS order_id_sum_amount
    FROM cleaned
    WHERE paid_at_period_2 >= '2025-02-01'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at_period_2 - LAG(paid_at_period_2) OVER (PARTITION BY status ORDER BY paid_at_period_2, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM metrics
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        status,
        paid_at_period_2,
        next_amount,
        SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at_period_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at_period_2) OVER (PARTITION BY status ORDER BY paid_at_period_2, id) IS NULL
                 OR paid_at_period_2 > LAG(paid_at_period_2) OVER (PARTITION BY status ORDER BY paid_at_period_2, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM islands
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at_period_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM gaps_2
)
SELECT
    id,
    status,
    paid_at_period_2,
    next_amount,
    island_id,
    is_new_session,
    island_id_2,
    CASE WHEN status RLIKE '^(SUCCESS|FAILED)$' THEN 'GROUP_A' WHEN status RLIKE '^(REFUNDED)$' THEN 'PRIORITY' ELSE 'CORE' END AS status_zone
FROM sessions_payments
ORDER BY id DESC NULLS FIRST;
