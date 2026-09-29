WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        created_at,
        coupon_codes,
        attributes,
        EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code,
        COALESCE(attributes['utm_source'], 'none') AS utm_source_or_default,
        TRANSFORM(
            FILTER(items, i -> i.promo IS NOT NULL),
            i -> STRUCT(i.product_id AS pid, ROUND(i.price * 0.9, 2) AS discounted_price)
        ) AS promo_lines,
        SIZE(FILTER(items, i -> i.qty >= 3)) AS bulk_lines,
        attributes['utm_source'] AS utm_source
    FROM carts
)
-- Running and moving aggregates
SELECT
    id,
    created_at,
    has_welcome_code,
    bulk_lines,
    PERCENT_RANK() OVER (ORDER BY bulk_lines NULLS FIRST) bulk_lines_percent_rank,
    AVG(bulk_lines) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) bulk_lines_avg_30m,
    LAG(bulk_lines, 1) OVER (ORDER BY bulk_lines, id) prev_bulk_lines
FROM parsed
WHERE bulk_lines BETWEEN 1 AND 3
