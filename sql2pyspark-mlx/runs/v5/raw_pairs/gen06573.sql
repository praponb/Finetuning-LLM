WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        COALESCE(amount, 0) AS amount_filled
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-10-26 12:00:00'
      AND method IS NOT NULL
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(status) AS status_lower,
        SUBSTRING(status, 1, 3) AS status_prefix
    FROM valid
    WHERE order_id IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method, order_id ORDER BY amount ASC, id ASC) AS dedup_rank
        FROM base
    ) ranked
    WHERE dedup_rank = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY dedup_rank ASC, id ASC) AS row_num
        FROM latest
    ) t
    WHERE row_num = 1
),
latest_payments AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id ASC) AS row_num_2
    FROM ranked
    QUALIFY row_num_2 = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        method,
        status,
        amount
    FROM latest_payments
) src
PIVOT (
    MAX(amount) AS value_sum,
    COUNT(1) AS cnt
    FOR status IN ('REFUNDED' AS refunded, 'SUCCESS' AS success)
)
