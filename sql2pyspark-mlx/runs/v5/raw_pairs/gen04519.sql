WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        HOUR(paid_at) AS paid_at_hour,
        SUBSTRING(status, 1, 4) AS status_prefix
    FROM payments
    WHERE method IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        paid_at_hour,
        CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END AS is_failed,
        COALESCE(method, 'N/A') AS resolved_method
    FROM filtered
    WHERE amount >= 530
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        status,
        paid_at_hour,
        TO_DATE(paid_at) AS paid_at_date,
        ROUND(amount * 1.15, 2) AS adjusted_amount
    FROM cleaned
    WHERE amount IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY paid_at_hour DESC NULLS FIRST, id) AS rn
        FROM base
    ) t
    WHERE rn = 1
)
SELECT
    id,
    order_id,
    status,
    paid_at_hour,
    paid_at_date,
    adjusted_amount,
    rn
FROM latest
ORDER BY id
LIMIT 20;
