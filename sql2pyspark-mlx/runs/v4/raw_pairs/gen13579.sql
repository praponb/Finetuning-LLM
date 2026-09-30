WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS k, v
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.email,
        p.name
    FROM exploded src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.city,
        b.segment
    FROM enriched src
    LEFT JOIN customers b
        ON src.customer_id = b.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        created_at,
        k,
        v,
        email,
        city,
        segment,
        SUBSTRING(email, 1, 4) AS email_prefix,
        TO_DATE(created_at) AS created_at_date
    FROM with_dims
    WHERE created_at >= DATE '2025-03-02'
      AND k IS NOT NULL
      AND segment = 'SMB'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        created_at,
        k,
        v,
        email,
        segment,
        email_prefix,
        created_at_date,
        SPLIT(LOWER(email), '@')[0] AS email_user,
        LAST_DAY(created_at_date) AS created_at_date_month_end
    FROM valid
    WHERE segment = 'Consumer'
)
SELECT
    created_at,
    k,
    v
FROM filtered
ORDER BY k DESC NULLS FIRST, created_at;
