WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(amount, 2) AS amount_rounded,
        UPPER(status) AS status_upper
    FROM payments
    WHERE paid_at >= TO_DATE('2023-06-30')
      AND method IS NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount_rounded DESC NULLS FIRST, id) AS latest_rank
        FROM filtered
    ) x
    WHERE latest_rank = 1
),
filtered_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(method, 1, 3) AS method_prefix
    FROM deduped
    WHERE paid_at > TIMESTAMP '2023-10-04 07:00:00'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, latest_rank - amount AS latest_rank_minus_amount
    FROM filtered_payments
    WHERE paid_at > TIMESTAMP '2023-09-03 17:00:00'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        order_id,
        status,
        method,
        latest_rank_minus_amount
    FROM base
) src
PIVOT (
    MAX(latest_rank_minus_amount) AS value_sum,
    COUNT(1) AS n
    FOR method IN ('promptpay' AS promptpay, 'cash' AS cash)
)
