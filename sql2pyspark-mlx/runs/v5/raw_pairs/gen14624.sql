WITH exploded AS (
    SELECT
        id,
        pos + 1 AS item_no,
        rec.product_id,
        rec.price
    FROM carts
    LATERAL VIEW POSEXPLODE(items) li AS pos, rec
),
base AS (
    SELECT *, product_id - item_no AS product_id_minus_item_no
    FROM exploded
    WHERE price <= 100
),
scored AS (
    SELECT
        *,
        NTILE(10) OVER (ORDER BY price DESC, id, item_no) AS price_bucket,
        SUM(product_id_minus_item_no) OVER (ORDER BY price, id, item_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_product_id_minus_item_no,
        SUM(item_no) OVER (ORDER BY product_id, id, item_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_item_no,
        SUM(price) OVER (ORDER BY product_id, id, item_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_price
    FROM base
    WHERE product_id != 3
),
enriched AS (
    SELECT
        *,
        LEAD(running_price, 2) OVER w_recent AS next_running_price,
        NTILE(3) OVER w_grp AS cumulative_product_id_minus_item_no_bucket,
        AVG(item_no) OVER w_part AS item_no_avg_5
    FROM scored
    WINDOW w_recent AS (ORDER BY running_price, id, item_no), w_grp AS (ORDER BY cumulative_product_id_minus_item_no, id, item_no), w_part AS (ORDER BY running_price, id, item_no ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
metrics AS (
    SELECT
        *,
        FIRST_VALUE(cumulative_product_id_minus_item_no_bucket) IGNORE NULLS OVER (ORDER BY item_no_avg_5, id, item_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_product_id_minus_item_no_bucket,
        NTILE(10) OVER (ORDER BY cumulative_product_id_minus_item_no_bucket DESC, id, item_no) AS cumulative_product_id_minus_item_no_bucket_bucket
    FROM enriched
),
metrics_carts AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY price NULLS LAST) AS price_dense_rank,
        LAST_VALUE(item_no) IGNORE NULLS OVER (ORDER BY item_no, id, item_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_item_no,
        LAST_VALUE(next_running_price) IGNORE NULLS OVER (ORDER BY cumulative_item_no, id, item_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_running_price,
        AVG(price) OVER (ORDER BY cumulative_product_id_minus_item_no_bucket, id, item_no ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS price_avg_3_prior,
        FIRST_VALUE(first_known_cumulative_product_id_minus_item_no_bucket) IGNORE NULLS OVER (ORDER BY product_id_minus_item_no, id, item_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_known_cumulative_product_id_minus_item_no_bucket
    FROM metrics
)
SELECT
    product_id,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(cumulative_item_no, ARRAY(0.25, 0.5, 0.75)) AS cumulative_item_no_quartiles,
    COUNT(DISTINCT id) AS distinct_ids
FROM metrics_carts
GROUP BY 1
ORDER BY distinct_ids DESC;
