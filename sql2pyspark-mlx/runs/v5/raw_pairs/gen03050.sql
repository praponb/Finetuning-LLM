WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS code
    FROM carts
),
base AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(code, 1, 4) AS code_prefix
    FROM flattened
    WHERE created_at >= TIMESTAMP '2025-03-02 01:00:00'
      AND customer_id IS NOT NULL
),
base_carts AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        created_at,
        code_prefix,
        DATE_FORMAT(created_at, 'yyyy-MM-dd') AS created_at_label,
        COALESCE(code, 'N/A') AS code_clean
    FROM base
    WHERE created_at > TIMESTAMP '2025-03-02 01:00:00'
      AND code IS DISTINCT FROM 'BUNDLE'
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.created_at,
        e.created_at_label,
        c.credit_limit,
        c.email
    FROM base_carts e
    LEFT JOIN customers c
        ON e.customer_id = c.id
)
SELECT
    id,
    customer_id,
    created_at_label,
    credit_limit
FROM joined;
