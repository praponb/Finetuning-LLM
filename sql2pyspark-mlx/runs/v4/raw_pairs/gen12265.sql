WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        CONCAT(method, ' - ', status) AS method_status,
        ROUND(amount, 1) AS amount_rounded
    FROM payments
    WHERE method IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        method_status,
        amount_rounded,
        LEAD(amount_rounded, 1) OVER (PARTITION BY status ORDER BY amount_rounded, id) AS next_amount_rounded,
        COUNT(amount_rounded) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_rounded_count_7d,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC, id) AS row_seq
    FROM valid
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM scored
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    id,
    method,
    status,
    method_status,
    amount_rounded,
    next_amount_rounded,
    amount_rounded_count_7d,
    row_seq,
    island_id,
    CASE WHEN method RLIKE '^(cash|bank_transfer)$' THEN 'GROUP_A' WHEN method RLIKE '^(card|paypal)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS method_zone
FROM sessions_payments
ORDER BY next_amount_rounded DESC, amount_rounded_count_7d
