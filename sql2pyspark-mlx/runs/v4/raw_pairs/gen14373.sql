WITH with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.brand,
        latest.product_price
    FROM suppliers s
    LEFT JOIN LATERAL (
            SELECT c.brand, c.price AS product_price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        brand,
        product_price,
        RANK() OVER (PARTITION BY country ORDER BY product_price DESC NULLS FIRST) AS product_price_rank,
        NTILE(5) OVER (PARTITION BY brand ORDER BY rating ASC, id ASC) AS rating_bucket,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY product_price ASC, id ASC) AS row_seq
    FROM with_latest
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, city
    FROM scored
    INTERSECT ALL
    SELECT id, city
    FROM suppliers
    WHERE country IN ('UK', 'DE')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.id = k.id AND s.city = k.city
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        brand,
        product_price,
        product_price_rank,
        rating_bucket,
        row_seq,
        COUNT(rating) OVER (PARTITION BY country, brand) AS country_count_rating,
        STDDEV_SAMP(rating) OVER (PARTITION BY country) AS country_stddev_samp_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city, country), 4) AS rating_share,
        ROUND(product_price / SUM(product_price) OVER (PARTITION BY city), 4) AS product_price_share
    FROM kept_rows
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN rating_share >= 0.76 THEN 'HIGH' ELSE 'LOW' END AS rating_share_tier,
        SIZE(COLLECT_SET(brand)) AS num_brands,
        COUNT(*) FILTER (WHERE brand = 'Umbrella') AS umbrella_rows,
        ROUND(SUM(product_price_share) / NULLIF(COUNT(CASE WHEN brand = 'Globex' THEN 1 END), 0), 2) AS product_price_share_per_globex
    FROM metrics
    GROUP BY CASE WHEN rating_share >= 0.76 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    rating_share_tier,
    num_brands,
    umbrella_rows,
    product_price_share_per_globex
FROM daily
ORDER BY num_brands DESC NULLS FIRST, umbrella_rows DESC
