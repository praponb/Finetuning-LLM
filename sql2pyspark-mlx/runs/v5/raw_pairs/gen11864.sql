WITH filtered AS (
    SELECT *, unit_price - quantity AS line_total
    FROM order_items
    WHERE product_id IN (6, 23, 36)
),
enriched AS (
    SELECT
        src.id,
        src.order_id,
        src.product_id,
        src.unit_price,
        d.category,
        d.cost
    FROM filtered src
    LEFT JOIN products d
        ON src.product_id = d.id
),
scored AS (
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        category,
        cost,
        NTILE(5) OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS unit_price_bucket,
        AVG(unit_price) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_avg_2_prior,
        SUM(unit_price) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price
    FROM enriched
),
base AS (
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        cost,
        unit_price_bucket,
        unit_price_avg_2_prior,
        ROUND(unit_price_avg_2_prior / cost, 2) AS unit_price_avg_2_prior_per_cost,
        COALESCE(category, 'UNKNOWN') AS category_filled
    FROM scored
    WHERE category IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        order_id,
        product_id,
        cost,
        unit_price_bucket,
        unit_price_avg_2_prior,
        category_filled,
        RANK() OVER (PARTITION BY product_id ORDER BY cost NULLS FIRST) AS cost_rank,
        ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY unit_price_avg_2_prior_per_cost DESC, id) AS event_seq,
        AVG(unit_price_avg_2_prior_per_cost) OVER (PARTITION BY product_id, order_id ORDER BY unit_price, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS unit_price_avg_2_prior_per_cost_avg_5,
        MAX(unit_price_bucket) OVER w_recent AS order_id_max_unit_price_bucket,
        AVG(unit_price_bucket) OVER (PARTITION BY product_id ORDER BY unit_price_bucket, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS unit_price_bucket_avg_6_prior
    FROM base
    WINDOW w_recent AS (PARTITION BY order_id)
)
SELECT
    id,
    order_id,
    product_id,
    cost,
    unit_price_bucket,
    unit_price_avg_2_prior,
    category_filled,
    cost_rank,
    event_seq
FROM with_windows;
