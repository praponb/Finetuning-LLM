WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        CAST(quantity AS DOUBLE) AS quantity_num,
        unit_price - quantity AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND quantity >= 5
),
joined AS (
    -- Attach products attributes
    SELECT
        src.*,
        b.product_name,
        b.category,
        b.stock_qty
    FROM filtered src
    LEFT JOIN products b
        ON src.product_id = b.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        line_total,
        PERCENT_RANK() OVER (PARTITION BY product_id, order_id ORDER BY quantity_num NULLS FIRST) AS quantity_num_percent_rank,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY category, product_id), 4) AS quantity_share
    FROM joined
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        quantity_num_percent_rank,
        LAG(quantity_num_percent_rank) OVER (PARTITION BY order_id, product_id ORDER BY line_total, id) AS prev_quantity_num_percent_rank,
        LEAD(quantity_share) OVER (PARTITION BY order_id, product_id ORDER BY line_total, id) AS next_quantity_share,
        LAG(quantity_share) OVER (PARTITION BY order_id, product_id ORDER BY line_total, id) AS prev_quantity_share,
        ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY line_total, id) AS event_seq,
        AVG(quantity_share) OVER (PARTITION BY product_id, order_id ORDER BY quantity_share, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS quantity_share_avg_6
    FROM metrics
)
-- Grouped metrics
SELECT
    product_id,
    CASE WHEN next_quantity_share >= 0.8 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_quantity_share_tier,
    CASE WHEN prev_quantity_num_percent_rank >= 0.87 THEN 'HIGH' ELSE 'LOW' END AS prev_quantity_num_percent_rank_tier,
    PERCENTILE_APPROX(prev_quantity_share, 0.5) AS prev_quantity_share_p50,
    ROUND(AVG(next_quantity_share), 2) AS avg_next_quantity_share,
    ROUND(AVG(quantity_share_avg_6), 2) AS avg_quantity_share_avg_6,
    SUM(prev_quantity_num_percent_rank) AS total_prev_quantity_num_percent_rank,
    AVG(quantity_num_percent_rank) AS avg_quantity_num_percent_rank,
    DENSE_RANK() OVER (PARTITION BY product_id ORDER BY SUM(prev_quantity_num_percent_rank) DESC) AS total_prev_quantity_num_percent_rank_dense_rank
FROM with_windows
GROUP BY product_id, CASE WHEN next_quantity_share >= 0.8 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN prev_quantity_num_percent_rank >= 0.87 THEN 'HIGH' ELSE 'LOW' END
ORDER BY avg_quantity_num_percent_rank DESC NULLS LAST;
