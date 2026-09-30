WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        pos,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) li AS pos, elem
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.city,
        p.email
    FROM flattened a
    JOIN customers p
        ON a.customer_id = p.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(pos) OVER (PARTITION BY city) AS city_max_pos,
        SUM(pos) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS pos_sum_30m,
        SUM(qty) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS qty_sum_30m
    FROM with_dims
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.name,
        p.age,
        p.country
    FROM scored s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        pos,
        product_id,
        qty,
        price,
        promo,
        city,
        email,
        city_max_pos,
        pos_sum_30m,
        qty_sum_30m,
        name,
        country,
        qty_sum_30m - product_id AS qty_sum_30m_minus_product_id,
        ROUND(city_max_pos, 1) AS city_max_pos_rounded,
        COALESCE(promo, 'N/A') AS promo_filled
    FROM joined
    WHERE created_at >= TO_DATE('2025-03-01')
      AND customer_id IS NOT NULL
),
scored_carts AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        pos,
        product_id,
        qty,
        price,
        promo,
        city,
        email,
        city_max_pos,
        pos_sum_30m,
        qty_sum_30m,
        name,
        country,
        qty_sum_30m_minus_product_id,
        city_max_pos_rounded,
        promo_filled,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY city_max_pos_rounded DESC NULLS LAST) AS city_max_pos_rounded_dense_rank,
        SUM(qty_sum_30m) OVER (ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS qty_sum_30m_sum_30m,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY price ASC) AS price_dense_rank,
        COUNT(product_id) OVER (PARTITION BY country ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS product_id_count_1h,
        ROUND(qty / SUM(qty) OVER (PARTITION BY promo), 4) AS qty_share
    FROM valid
),
scored_carts_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER w_part AS product_id_count_1h_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY pos DESC NULLS FIRST) AS pos_percent_rank,
        RANK() OVER w_ordered AS pos_rank,
        AVG(city_max_pos_rounded) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS city_max_pos_rounded_avg_30m,
        ROUND(qty_sum_30m / SUM(qty_sum_30m) OVER (PARTITION BY promo), 4) AS qty_sum_30m_share
    FROM scored_carts
    WINDOW w_part AS (PARTITION BY customer_id ORDER BY product_id_count_1h DESC NULLS LAST), w_ordered AS (PARTITION BY country ORDER BY pos DESC NULLS FIRST)
)
SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    pos,
    promo,
    city_max_pos,
    pos_sum_30m,
    qty_sum_30m
FROM scored_carts_2
ORDER BY id DESC NULLS LAST;
