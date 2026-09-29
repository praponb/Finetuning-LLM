WITH flattened AS (
    SELECT
        id,
        elem.product_id,
        elem.price,
        elem.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) t AS elem
),
cleaned AS (
    SELECT *, LOWER(promo) AS promo_lower
    FROM flattened
    WHERE product_id <= 9
),
filtered AS (
    SELECT
        *,
        COALESCE(promo, 'Unknown') AS promo_filled,
        GREATEST(product_id, price) AS greatest_product_id_price,
        COALESCE(promo, 'OTHER') AS promo_filled_2
    FROM cleaned
    WHERE product_id BETWEEN 6 AND 7
),
with_windows AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY greatest_product_id_price) AS greatest_product_id_price_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY greatest_product_id_price NULLS LAST) AS greatest_product_id_price_percent_rank_2,
        STDDEV_SAMP(greatest_product_id_price) OVER (PARTITION BY promo) AS promo_stddev_samp_greatest_product_id_price
    FROM filtered
    WHERE price <= 230
),
valid AS (
    SELECT *, UPPER(promo) AS promo_upper
    FROM with_windows
    WHERE promo IS NOT NULL
      AND price > 210
),
valid_carts AS (
    SELECT
        id,
        product_id,
        price,
        promo,
        promo_lower,
        promo_filled,
        greatest_product_id_price,
        greatest_product_id_price_percent_rank,
        greatest_product_id_price_percent_rank_2,
        promo_upper,
        LENGTH(promo) AS promo_length,
        COALESCE(promo, 'DIRECT') AS resolved_promo
    FROM valid
    WHERE promo IS NOT NULL
)
SELECT
    promo,
    SIZE(COLLECT_SET(promo)) AS num_promos,
    COUNT(DISTINCT promo) AS distinct_promos
FROM valid_carts
GROUP BY promo
HAVING num_promos > 1
ORDER BY promo
