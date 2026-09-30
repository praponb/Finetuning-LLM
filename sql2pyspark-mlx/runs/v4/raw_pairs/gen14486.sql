WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        elem.product_id
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) li AS elem
),
enriched AS (
    SELECT s.*, b.credit_limit
    FROM flattened s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
metrics AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY product_id NULLS LAST) AS product_id_cume_dist,
        MIN(credit_limit) OVER (PARTITION BY customer_id) AS customer_id_min_credit_limit
    FROM enriched
),
metrics_carts AS (
    SELECT
        *,
        COUNT(product_id_cume_dist) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS product_id_cume_dist_count_2h,
        MIN(product_id_cume_dist) OVER (PARTITION BY customer_id) AS customer_id_min_product_id_cume_dist,
        MAX(product_id_cume_dist) OVER (PARTITION BY customer_id) AS customer_id_max_product_id_cume_dist
    FROM metrics
),
valid AS (
    SELECT *, ROUND(product_id_cume_dist_count_2h * 100, 2) AS product_id_cume_dist_count_2h_adjusted
    FROM metrics_carts
    WHERE customer_id_max_product_id_cume_dist BETWEEN 0 AND 1
),
scored AS (
    SELECT
        *,
        MAX(customer_id_min_product_id_cume_dist) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS customer_id_min_product_id_cume_dist_max_1h,
        MAX(product_id_cume_dist_count_2h) OVER w_grp AS customer_id_max_product_id_cume_dist_count_2h,
        MAX(customer_id_max_product_id_cume_dist) OVER w_grp AS customer_id_max_customer_id_max_product_id_cume_dist,
        SUM(product_id_cume_dist_count_2h_adjusted) OVER w_grp AS customer_id_sum_product_id_cume_dist_count_2h_adjusted
    FROM valid
    WINDOW w_grp AS (PARTITION BY customer_id)
),
joined AS (
    SELECT e.*, p.signup_date
    FROM scored e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
grouped AS (
    SELECT
        product_id,
        DATE_TRUNC('HOUR', created_at) AS hour_window,
        COUNT(*) AS num_rows,
        COUNT(*) AS cnt,
        MAX(created_at) AS last_created_at,
        MAX(created_at) AS last_seen
    FROM joined
    GROUP BY product_id, DATE_TRUNC('HOUR', created_at)
)
SELECT
    product_id,
    hour_window,
    cnt,
    last_created_at
FROM grouped
ORDER BY product_id, last_created_at
