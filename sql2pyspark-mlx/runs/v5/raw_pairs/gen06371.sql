WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        INLINE(items)
    FROM carts
),
exploded_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        product_id,
        qty,
        price,
        promo,
        value
    FROM exploded
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS value
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        promo,
        price + qty price_plus_qty,
        SUBSTRING(value, 1, 4) value_prefix
    FROM exploded_carts
    WHERE promo IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        promo,
        price_plus_qty,
        value_prefix,
        CUME_DIST() OVER w_part price_plus_qty_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY price_plus_qty DESC NULLS FIRST) price_plus_qty_percent_rank,
        RANK() OVER w_grp price_plus_qty_rank
    FROM base
    WHERE promo <> 'SPRING10'
    WINDOW w_part AS (ORDER BY price_plus_qty DESC NULLS FIRST), w_grp AS (ORDER BY price_plus_qty DESC)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        promo,
        price_plus_qty,
        price_plus_qty_percent_rank,
        price_plus_qty_cume_dist
    FROM scored
) src
UNPIVOT (
    metric_value FOR measure IN (price_plus_qty, price_plus_qty_percent_rank, price_plus_qty_cume_dist)
) long_rows;
