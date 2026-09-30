WITH scored AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        LAG(rating, 1) OVER (ORDER BY rating, id) prev_rating,
        NTILE(3) OVER w_hist rating_bucket,
        AVG(rating) OVER w_ordered country_avg_rating
    FROM suppliers
    WINDOW w_hist AS (PARTITION BY country ORDER BY rating DESC, id), w_ordered AS (PARTITION BY country)
),
metrics AS (
    SELECT
        *,
        AVG(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_avg_rating,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_sum_rating,
        COUNT(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_count_rating,
        RANK() OVER (PARTITION BY country, city ORDER BY rating_bucket DESC) rating_bucket_rank
    FROM scored
),
latest_match AS (
    SELECT
        s.id,
        s.country,
        s.rating,
        s.rating_bucket,
        s.country_avg_rating,
        s.last3_sum_rating,
        s.last3_count_rating,
        s.rating_bucket_rank,
        top1.product_brand,
        top1.product_price
    FROM metrics s
    JOIN LATERAL (
            SELECT c.brand AS product_brand, c.price AS product_price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored_suppliers AS (
    SELECT
        *,
        MIN(rating_bucket) OVER (PARTITION BY country ORDER BY product_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_min_rating_bucket,
        MAX(rating_bucket) OVER (PARTITION BY country ORDER BY product_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_max_rating_bucket,
        COUNT(rating_bucket) OVER (PARTITION BY country ORDER BY product_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_count_rating_bucket,
        NTILE(10) OVER (PARTITION BY country, product_brand ORDER BY country_avg_rating DESC, id) country_avg_rating_bucket
    FROM latest_match
),
with_windows AS (
    SELECT
        id,
        last3_sum_rating,
        rating_bucket_rank,
        last3_min_rating_bucket,
        last3_max_rating_bucket,
        LAG(last3_sum_rating) OVER w_running prev_last3_sum_rating,
        LEAD(last3_sum_rating) OVER w_running next_last3_sum_rating,
        LAG(country_avg_rating_bucket) OVER w_running prev_country_avg_rating_bucket,
        ROW_NUMBER() OVER w_running seq_no
    FROM scored_suppliers
    WINDOW w_running AS (PARTITION BY country ORDER BY country_avg_rating_bucket, id)
),
metrics_suppliers AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_running seq_no_2,
        ROW_NUMBER() OVER (ORDER BY seq_no DESC, id) seq_no_3,
        LAST_VALUE(last3_sum_rating) IGNORE NULLS OVER w_grp last_known_last3_sum_rating,
        NTILE(5) OVER (ORDER BY last3_sum_rating DESC, id) last3_sum_rating_bucket,
        LEAD(rating_bucket_rank, 1) OVER (ORDER BY last3_max_rating_bucket, id) next_rating_bucket_rank
    FROM with_windows
    WINDOW w_running AS (ORDER BY next_last3_sum_rating, id), w_grp AS (ORDER BY next_last3_sum_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    last3_sum_rating,
    last3_max_rating_bucket,
    prev_last3_sum_rating,
    next_last3_sum_rating,
    prev_country_avg_rating_bucket,
    seq_no_2,
    seq_no_3,
    last_known_last3_sum_rating
FROM metrics_suppliers
ORDER BY next_last3_sum_rating, id DESC;
