WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM (
        SELECT
            id,
            customer_id,
            coupon_codes,
            EXPLODE(items) AS li_item
        FROM carts
    ) e
),
lines_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        price,
        promo,
        tag
    FROM lines
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS tag
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.promo,
        s.tag,
        p.segment,
        p.country
    FROM lines_carts s
    INNER JOIN customers p
        ON s.customer_id = p.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        promo,
        tag,
        segment,
        country,
        LENGTH(segment) segment_length,
        CONCAT(country, '/', segment) country_segment
    FROM enriched
    WHERE country IS NOT NULL
      AND tag NOT IN ('WELCOME5', 'VIP')
)
SELECT
    s.id,
    s.customer_id,
    s.segment_length,
    s.country_segment,
    (SELECT AVG(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM base s
