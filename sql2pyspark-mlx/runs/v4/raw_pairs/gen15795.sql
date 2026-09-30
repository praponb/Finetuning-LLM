WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        COALESCE(amount, 0) AS amount_filled,
        CAST(amount AS INT) AS amount_int,
        COALESCE(method, 'DIRECT') AS method_clean
    FROM payments
    WHERE paid_at > TO_TIMESTAMP('2024-05-01 22:00:00')
      AND method IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT e.*, p.order_date
    FROM cleaned e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
enriched_payments AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        amount_filled,
        amount_int,
        method_clean,
        order_date,
        NTILE(3) OVER (ORDER BY amount_int, id) AS amount_int_bucket,
        LEAD(amount_filled, 1) OVER (PARTITION BY method ORDER BY paid_at, id) AS next_amount_filled,
        SUM(amount_int) OVER (PARTITION BY method, order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_int_sum_90d
    FROM enriched
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched_payments
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at NULLS LAST, id) AS row_num
    FROM sessions_payments
    QUALIFY row_num = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        status,
        next_amount_filled,
        amount_filled
    FROM deduped
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric IN (next_amount_filled, amount_filled)
);
