WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        pos,
        elem.product_id,
        elem.qty,
        elem.price
    FROM carts
    LATERAL VIEW POSEXPLODE(items) li AS pos, elem
),
flattened_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        pos,
        product_id,
        qty,
        price,
        code
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) li AS code
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.pos,
        a.product_id,
        a.price,
        a.code,
        b.signup_date
    FROM flattened_carts a
    LEFT JOIN customers b
        ON a.customer_id <=> b.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        customer_id,
        pos,
        product_id,
        price,
        code,
        AVG(price) OVER (PARTITION BY customer_id, code) AS customer_id_avg_price,
        CUME_DIST() OVER w_grp AS price_cume_dist,
        SUM(product_id) OVER w_grp_2 AS customer_id_sum_product_id
    FROM joined
    WHERE code = 'WELCOME5'
    WINDOW w_grp AS (PARTITION BY customer_id ORDER BY price DESC), w_grp_2 AS (PARTITION BY customer_id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(pos) OVER (PARTITION BY code) AS code_count_pos,
        RANK() OVER (ORDER BY product_id DESC) AS product_id_rank,
        DENSE_RANK() OVER (ORDER BY price_cume_dist NULLS LAST) AS price_cume_dist_dense_rank
    FROM scored
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        product_id,
        code,
        CASE WHEN price >= 195 THEN 'LARGE' ELSE 'SMALL' END AS price_tier,
        COUNT(*) AS num_rows,
        COUNT(*) AS cnt,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE code = 'SPRING10') AS spring10_rows,
        ROUND(SUM(price) / NULLIF(COUNT(CASE WHEN code = 'SPRING10' THEN 1 END), 0), 2) AS price_per_spring10
    FROM metrics
    GROUP BY 1, 2, 3
)
SELECT
    product_id,
    code,
    price_tier,
    cnt,
    row_count,
    spring10_rows,
    price_per_spring10
FROM totals
ORDER BY cnt DESC NULLS LAST;
