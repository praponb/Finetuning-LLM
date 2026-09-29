WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS code
    FROM carts
),
with_dims AS (
    -- Attach customers attributes
    SELECT src.*, p.country
    FROM exploded src
    INNER JOIN customers p
        ON src.customer_id <=> p.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.country AS customer_country,
        p.email,
        p.city
    FROM with_dims src
    LEFT JOIN customers p
        ON src.customer_id = p.id
    WHERE p.signup_date < '2024-07-01'
),
joined_carts AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.created_at,
        e.code,
        e.customer_country,
        e.email,
        b.signup_date
    FROM joined e
    INNER JOIN customers b
        ON e.customer_id = b.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        code,
        email,
        TRIM(code) AS clean_code,
        LOWER(code) AS code_lower,
        COALESCE(code, 'OTHER') AS code_clean
    FROM joined_carts
    WHERE created_at >= TO_DATE('2025-03-01')
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        REGEXP_REPLACE(email, ' ', '-') AS email_slug,
        COALESCE(code, 'NONE') AS code_filled
    FROM filtered
    WHERE created_at > TO_DATE('2025-03-01')
      AND code IS NOT DISTINCT FROM 'WELCOME5'
)
SELECT
    created_at,
    email,
    code_lower,
    code_clean,
    email_slug,
    code_filled
FROM base
