WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM carts
),
with_dims AS (
    -- Attach customers attributes
    SELECT src.*, b.credit_limit
    FROM flattened src
    LEFT JOIN customers b
        ON src.customer_id <=> b.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        customer_id,
        created_at,
        credit_limit,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY customer_id, element), 4) AS credit_limit_share
    FROM with_dims
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(credit_limit) AS credit_limit_whole
    FROM with_windows
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 17:00:00')
      AND customer_id IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        credit_limit_share,
        credit_limit_whole,
        RANK() OVER (PARTITION BY customer_id ORDER BY credit_limit DESC) AS credit_limit_rank,
        RANK() OVER (PARTITION BY customer_id ORDER BY credit_limit_whole NULLS LAST) AS credit_limit_whole_rank
    FROM base
)
SELECT
    credit_limit_whole,
    credit_limit_rank,
    credit_limit_whole_rank
FROM enriched
