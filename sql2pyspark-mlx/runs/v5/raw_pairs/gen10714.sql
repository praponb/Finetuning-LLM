WITH with_last AS (
    SELECT
        s.id,
        s.supplier_name,
        s.rating,
        x.category,
        x.product_price
    FROM suppliers s
    LEFT JOIN LATERAL (
            SELECT c.category, c.price AS product_price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    SELECT
        id,
        supplier_name,
        category,
        product_price,
        SUM(rating) OVER (PARTITION BY category ORDER BY product_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating,
        ROW_NUMBER() OVER w_part AS row_seq,
        ROUND(product_price / SUM(product_price) OVER (PARTITION BY category), 4) AS product_price_share,
        LAST_VALUE(product_price, TRUE) OVER (PARTITION BY category ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_product_price,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY rating DESC) AS rating_dense_rank
    FROM with_last
    WINDOW w_part AS (PARTITION BY category ORDER BY product_price DESC, id)
),
with_latest AS (
    SELECT
        s.id,
        s.supplier_name,
        s.category,
        s.rating_dense_rank,
        x.product_brand
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    SELECT
        id,
        supplier_name,
        category,
        product_brand,
        STDDEV_SAMP(rating_dense_rank) OVER (PARTITION BY product_brand) AS product_brand_stddev_samp_rating_dense_rank,
        NTILE(5) OVER (ORDER BY rating_dense_rank, id) AS rating_dense_rank_bucket,
        ROUND(rating_dense_rank / SUM(rating_dense_rank) OVER (PARTITION BY category, product_brand), 4) AS rating_dense_rank_share
    FROM with_latest
)
SELECT
    CASE WHEN product_brand_stddev_samp_rating_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS product_brand_stddev_samp_rating_dense_rank_tier,
    CASE WHEN rating_dense_rank_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END AS rating_dense_rank_bucket_tier,
    AVG(rating_dense_rank_share) FILTER (WHERE product_brand = 'Stark') AS stark_avg_rating_dense_rank_share,
    COUNT_IF(product_brand = 'Umbrella') AS umbrella_events,
    AVG(rating_dense_rank_share) FILTER (WHERE category = 'Books') AS books_avg_rating_dense_rank_share,
    COUNT(*) FILTER (WHERE category = 'Books') AS books_rows,
    SIZE(COLLECT_SET(product_brand)) AS num_product_brands
FROM with_windows
GROUP BY CASE WHEN product_brand_stddev_samp_rating_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN rating_dense_rank_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END
ORDER BY books_avg_rating_dense_rank_share
