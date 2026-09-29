WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS tag
    FROM carts
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(tag)) AS tag_title,
        COALESCE(tag, 'DIRECT') AS tag_filled
    FROM lines
    WHERE created_at > TO_DATE('2025-03-01')
      AND tag IS DISTINCT FROM 'SPRING10'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        created_at,
        tag,
        tag_title,
        CASE WHEN tag IN ('SPRING10', 'VIP') THEN 'Tier 1' ELSE 'Other' END AS tag_group
    FROM base
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 22:00:00')
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.signup_date,
        c.city
    FROM valid a
    INNER JOIN customers c
        ON a.customer_id = c.id
    WHERE c.city != 'Chiang Mai'
)
SELECT
    s.id,
    s.customer_id,
    s.created_at,
    s.tag_group,
    s.signup_date,
    s.city,
    (SELECT SUM(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS sum_account_balance
FROM with_dims s
