WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        pos,
        li_item.product_id,
        li_item.price,
        li_item.promo
    FROM carts
    LATERAL VIEW POSEXPLODE(items) li AS pos, li_item
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(product_id) OVER (PARTITION BY customer_id ORDER BY product_id, id, pos ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS product_id_avg_6_prior,
        MIN(pos) OVER (PARTITION BY promo) AS promo_min_pos
    FROM lines
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        created_at,
        coupon_codes,
        pos,
        product_id,
        price,
        promo,
        product_id_avg_6_prior,
        promo_min_pos,
        TO_DATE(created_at) AS created_at_date
    FROM metrics
    WHERE HOUR(created_at) BETWEEN 12 AND 17
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(promo_min_pos) OVER (ORDER BY promo_min_pos, id, pos ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS promo_min_pos_avg_2,
        AVG(price) OVER w_running AS price_avg_2,
        SUM(product_id) OVER w_ordered AS running_product_id
    FROM valid
    WINDOW w_running AS (PARTITION BY promo ORDER BY product_id, id, pos ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY promo ORDER BY product_id_avg_6_prior, id, pos ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    coupon_codes,
    pos,
    product_id,
    price,
    promo,
    product_id_avg_6_prior,
    promo_min_pos,
    created_at_date
FROM enriched;
