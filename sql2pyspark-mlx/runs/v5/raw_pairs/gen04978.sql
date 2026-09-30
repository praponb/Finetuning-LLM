WITH flattened AS (
    SELECT
        c.id,
        c.coupon_codes,
        li_item.product_id,
        li_item.qty,
        li_item.price
    FROM carts c
    LATERAL VIEW EXPLODE(c.items) t AS li_item
),
scored AS (
    SELECT
        *,
        RANK() OVER (ORDER BY price DESC NULLS LAST) AS price_rank,
        RANK() OVER (ORDER BY product_id ASC) AS product_id_rank,
        CUME_DIST() OVER (ORDER BY product_id DESC NULLS LAST) AS product_id_cume_dist,
        RANK() OVER (ORDER BY price DESC) AS price_rank_2,
        CUME_DIST() OVER (ORDER BY price DESC NULLS LAST) AS price_cume_dist
    FROM flattened
),
matched_keys AS (
    SELECT id
    FROM scored
    EXCEPT
    SELECT id
    FROM carts
    WHERE customer_id IS NULL
),
verified AS (
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.id = k.id
),
metrics AS (
    SELECT
        id,
        product_id_cume_dist,
        DENSE_RANK() OVER (ORDER BY price DESC) AS price_dense_rank,
        CUME_DIST() OVER (ORDER BY price_rank DESC NULLS LAST) AS price_rank_cume_dist,
        RANK() OVER w_ordered AS product_id_rank_2
    FROM verified
    WINDOW w_ordered AS (ORDER BY product_id ASC NULLS LAST)
),
with_windows AS (
    SELECT
        id,
        product_id_cume_dist,
        price_dense_rank,
        price_rank_cume_dist,
        product_id_rank_2,
        DENSE_RANK() OVER (ORDER BY product_id_cume_dist DESC NULLS FIRST) AS product_id_cume_dist_dense_rank,
        RANK() OVER (ORDER BY price_rank_cume_dist DESC NULLS FIRST) AS price_rank_cume_dist_rank,
        RANK() OVER (ORDER BY price_rank_cume_dist ASC NULLS LAST) AS price_rank_cume_dist_rank_2
    FROM metrics
)
SELECT
    CASE WHEN product_id_rank_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS product_id_rank_2_tier,
    CASE WHEN price_rank_cume_dist_rank_2 >= 8 THEN 'LARGE' ELSE 'SMALL' END AS price_rank_cume_dist_rank_2_tier,
    APPROX_PERCENTILE(product_id_cume_dist_dense_rank, 0.9) AS product_id_cume_dist_dense_rank_p90,
    PERCENTILE_APPROX(price_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS price_dense_rank_quartiles,
    APPROX_PERCENTILE(price_rank_cume_dist_rank, 0.5) AS price_rank_cume_dist_rank_p50,
    APPROX_PERCENTILE(price_rank_cume_dist_rank, ARRAY(0.25, 0.5, 0.75)) AS price_rank_cume_dist_rank_quartiles,
    PERCENTILE_APPROX(price_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS price_dense_rank_quartiles_2,
    RANK() OVER (PARTITION BY CASE WHEN product_id_rank_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY APPROX_PERCENTILE(price_rank_cume_dist_rank, 0.5) DESC) AS price_rank_cume_dist_rank_p50_rank
FROM with_windows
GROUP BY 1, 2
ORDER BY price_rank_cume_dist_rank_p50 DESC NULLS LAST
