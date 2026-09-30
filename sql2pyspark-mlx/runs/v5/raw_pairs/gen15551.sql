WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        entry.product_id,
        entry.qty,
        entry.price,
        entry.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) e AS entry
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM exploded
    EXCEPT
    SELECT customer_id, id
    FROM carts
    WHERE customer_id = 23
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(price) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS price_sum_1h,
        PERCENT_RANK() OVER w_part AS product_id_percent_rank
    FROM reconciled
    WINDOW w_part AS (PARTITION BY promo ORDER BY product_id NULLS FIRST)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        created_at,
        product_id,
        promo,
        LOWER(promo) AS promo_lower,
        COALESCE(promo, 'UNKNOWN') AS resolved_promo
    FROM metrics
    WHERE created_at > TIMESTAMP '2025-03-02 03:00:00'
),
metrics_carts AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(product_id) OVER (PARTITION BY promo) AS promo_sum_product_id,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY product_id NULLS LAST) AS product_id_dense_rank,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY product_id DESC NULLS LAST) AS product_id_dense_rank_2,
        MAX(product_id) OVER (PARTITION BY customer_id) AS customer_id_max_product_id
    FROM valid
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(customer_id_max_product_id / SUM(customer_id_max_product_id) OVER (PARTITION BY customer_id), 4) AS customer_id_max_product_id_share,
        RANK() OVER (ORDER BY product_id_dense_rank DESC NULLS FIRST) AS product_id_dense_rank_rank,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY customer_id_max_product_id DESC) AS customer_id_max_product_id_cume_dist
    FROM metrics_carts
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        promo,
        promo_lower,
        promo_sum_product_id,
        product_id_dense_rank,
        product_id_dense_rank_2,
        customer_id_max_product_id,
        customer_id_max_product_id_share,
        DATE_TRUNC('MONTH', created_at) AS created_at_period
    FROM enriched
    WHERE created_at > TO_TIMESTAMP('2025-03-01 22:00:00')
      AND promo IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    product_id_dense_rank,
    product_id_dense_rank_2,
    customer_id_max_product_id,
    AVG(customer_id_max_product_id_share) OVER (PARTITION BY customer_id) AS customer_id_avg_customer_id_max_product_id_share
FROM filtered;
