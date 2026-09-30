WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY paid_at DESC NULLS FIRST) AS latest_rank
    FROM payments
    QUALIFY latest_rank = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(amount AS INT) AS amount_int,
        COALESCE(method, 'N/A') AS method_clean
    FROM deduped
    WHERE order_id IS NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
deduped_payments AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        status,
        amount_int
    FROM sessions_payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY paid_at DESC, id) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        status,
        LAG(amount_int) OVER (PARTITION BY status ORDER BY amount_int, id) AS prev_amount_int,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount_int, id) AS seq_no,
        LAG(amount_int, 2) OVER (ORDER BY amount_int, id) AS prev_amount_int_2
    FROM deduped_payments
    WHERE amount_int <= 32
)
-- Multi-level aggregation
SELECT
    seq_no,
    status,
    COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success_rows,
    MIN(prev_amount_int_2) AS min_prev_amount_int_2,
    ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END), 0), 2) AS seq_no_per_refunded,
    COUNT(*) AS cnt,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    ROUND(AVG(prev_amount_int_2), 2) AS avg_prev_amount_int_2,
    RANK() OVER (PARTITION BY seq_no ORDER BY ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END), 0), 2) DESC) AS seq_no_per_refunded_rank
FROM enriched
GROUP BY 1, 2
