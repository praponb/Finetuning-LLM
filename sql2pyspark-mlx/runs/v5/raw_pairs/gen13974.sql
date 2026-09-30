WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS value
),
base AS (
    SELECT
        *,
        DATE_FORMAT(created_at, 'yyyy-MM') AS created_at_label,
        COALESCE(value, 'N/A') AS resolved_value
    FROM lines
    WHERE customer_id IS NOT NULL
      AND value IS DISTINCT FROM 'WELCOME5'
),
valid AS (
    SELECT
        *,
        CASE WHEN value = 'VIP' THEN 1 ELSE 0 END AS is_vip,
        CAST(created_at AS DATE) AS created_at_day
    FROM base
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 15:00:00')
      AND customer_id IS NULL
),
with_dims AS (
    SELECT
        src.customer_id,
        src.created_at,
        src.created_at_label,
        src.created_at_day,
        c.credit_limit
    FROM valid src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
cleaned AS (
    SELECT
        customer_id,
        created_at,
        created_at_day,
        credit_limit,
        CAST(credit_limit AS INT) AS credit_limit_int
    FROM with_dims
    WHERE TO_DATE(created_at) >= '2025-04-19'
      AND created_at > DATE '2025-03-01'
),
with_dims_carts AS (
    SELECT src.*, p.age
    FROM cleaned src
    LEFT JOIN customers p
        ON src.customer_id = p.id
)
SELECT
    DATE_TRUNC('WEEK', created_at_day) AS week_start,
    MAX(created_at) AS last_created_at,
    MIN(created_at) AS started_at
FROM with_dims_carts
GROUP BY DATE_TRUNC('WEEK', created_at_day)
ORDER BY week_start NULLS LAST
