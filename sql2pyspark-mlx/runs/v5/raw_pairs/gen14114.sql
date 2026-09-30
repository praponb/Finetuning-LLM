WITH joined AS (
    -- Attach products attributes
    SELECT
        s.*,
        b.product_name,
        b.price,
        b.stock_qty
    FROM order_items s
    LEFT JOIN products b
        ON s.product_id = b.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(stock_qty) OVER w_part AS running_sum_stock_qty,
        COUNT(stock_qty) OVER w_part AS running_count_stock_qty,
        MAX(stock_qty) OVER w_part AS running_max_stock_qty,
        AVG(stock_qty) OVER w_part AS running_avg_stock_qty
    FROM joined
    WINDOW w_part AS (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        quantity,
        price,
        stock_qty,
        running_sum_stock_qty,
        running_avg_stock_qty
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id, order_id ORDER BY running_sum_stock_qty DESC) AS rn
        FROM enriched
    ) x
    WHERE rn = 1
),
enriched_order_items AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY running_sum_stock_qty) AS running_sum_stock_qty_rank,
        LAG(running_sum_stock_qty, 1) OVER (ORDER BY running_sum_stock_qty, id) AS prev_running_sum_stock_qty,
        FIRST_VALUE(running_sum_stock_qty, TRUE) OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_sum_stock_qty
    FROM deduped
),
enriched_order_items_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY stock_qty) AS stock_qty_rank,
        NTILE(10) OVER (ORDER BY first_known_running_sum_stock_qty DESC, id) AS first_known_running_sum_stock_qty_bucket
    FROM enriched_order_items
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(stock_qty_rank * 0.9, 2) AS stock_qty_rank_adjusted
    FROM enriched_order_items_2
    WHERE stock_qty >= 241
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN quantity >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
        CASE WHEN running_sum_stock_qty >= 2210 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_sum_stock_qty_tier,
        PERCENTILE_APPROX(stock_qty_rank_adjusted, 0.5) AS stock_qty_rank_adjusted_p50,
        COUNT(*) AS event_count,
        BIT_OR(quantity) AS quantity_bit_or,
        SUM(prev_running_sum_stock_qty) AS total_prev_running_sum_stock_qty,
        GROUPING_ID() AS grouping_level
    FROM valid
    GROUP BY CUBE (CASE WHEN quantity >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN running_sum_stock_qty >= 2210 THEN 'PREMIUM' ELSE 'STANDARD' END)
    HAVING stock_qty_rank_adjusted_p50 > 0
)
SELECT
    running_sum_stock_qty_tier,
    stock_qty_rank_adjusted_p50,
    event_count,
    quantity_bit_or,
    total_prev_running_sum_stock_qty,
    grouping_level
FROM grouped
ORDER BY quantity_bit_or NULLS FIRST;
