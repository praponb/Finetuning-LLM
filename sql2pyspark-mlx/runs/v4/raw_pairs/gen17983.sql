WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        INLINE(items)
    FROM carts
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        product_id,
        qty,
        promo,
        code
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) x AS code
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY code, promo ORDER BY product_id ASC NULLS FIRST) AS product_id_cume_dist,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY product_id DESC NULLS LAST) AS product_id_dense_rank,
        RANK() OVER (PARTITION BY customer_id ORDER BY qty ASC NULLS FIRST) AS qty_rank,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY product_id DESC) AS product_id_dense_rank_2
    FROM exploded
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.city,
        p.name,
        p.segment
    FROM enriched a
    INNER JOIN customers p
        ON a.customer_id = p.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.country,
        b.segment AS customer_segment,
        b.credit_limit
    FROM with_dims e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
enriched_carts AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.signup_date,
        c.email,
        c.country AS customer_country
    FROM joined src
    INNER JOIN customers c
        ON src.customer_id = c.id
)
-- Ranking and gap features
SELECT
    *,
    ROUND(qty_rank / SUM(qty_rank) OVER (PARTITION BY segment), 4) AS qty_rank_share,
    DENSE_RANK() OVER w_recent AS product_id_cume_dist_dense_rank
FROM enriched_carts
WINDOW w_recent AS (ORDER BY product_id_cume_dist ASC NULLS LAST);
