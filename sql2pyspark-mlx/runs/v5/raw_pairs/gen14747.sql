WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(amount, 0) AS amount_filled
    FROM payments
    WHERE method IN ('cash', 'paypal')
      AND paid_at > TIMESTAMP '2023-11-12 02:00:00'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        amount_filled
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount_filled DESC, id) AS latest_rank
        FROM cleaned
    ) t
    WHERE latest_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(amount_filled) OVER (PARTITION BY status ORDER BY amount_filled, id) AS next_amount_filled,
        LAG(amount_filled) OVER (PARTITION BY status ORDER BY amount_filled, id) AS prev_amount_filled,
        LEAD(amount) OVER (PARTITION BY status ORDER BY amount_filled, id) AS next_amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount_filled, id) AS seq_no
    FROM deduped
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM scored
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        method,
        paid_at,
        prev_amount_filled,
        next_amount,
        seq_no,
        SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
SELECT
    id,
    method,
    paid_at,
    prev_amount_filled,
    seq_no,
    session_id,
    CASE WHEN method RLIKE '^(bank_transfer|cash)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS method_zone
FROM islands
ORDER BY prev_amount_filled NULLS LAST;
