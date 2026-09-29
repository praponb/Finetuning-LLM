WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity * unit_price AS line_total
    FROM order_items
    WHERE order_id IN (10, 16)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity ASC NULLS FIRST, id ASC) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        unit_price,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY product_id), 4) AS quantity_share,
        SUM(unit_price) OVER (PARTITION BY product_id ORDER BY line_total ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        LEAD(line_total, 2) OVER (ORDER BY line_total ASC, id ASC) AS next_line_total,
        CORR(unit_price, quantity) OVER (PARTITION BY order_id, product_id) AS corr_unit_price_quantity,
        RANK() OVER (PARTITION BY product_id, order_id ORDER BY quantity DESC NULLS LAST) AS quantity_rank
    FROM deduped
    WHERE unit_price < 370
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        unit_price,
        quantity_share,
        running_unit_price,
        next_line_total,
        corr_unit_price_quantity,
        quantity_rank,
        SUM(unit_price) OVER (PARTITION BY order_id ORDER BY quantity_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_unit_price,
        MIN(unit_price) OVER (PARTITION BY order_id ORDER BY quantity_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_unit_price,
        COUNT(unit_price) OVER (PARTITION BY order_id ORDER BY quantity_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_unit_price,
        PERCENT_RANK() OVER (ORDER BY quantity_share DESC NULLS LAST) AS quantity_share_percent_rank
    FROM enriched
),
base_order_items AS (
    -- Basic cleansing and defaults
    SELECT *, corr_unit_price_quantity - quantity_share_percent_rank AS corr_unit_price_quantity_minus_quantity_share_percent_rank
    FROM with_windows
    WHERE quantity_rank < 7
),
enriched_order_items AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(running_count_unit_price) OVER w_part AS last4_sum_running_count_unit_price,
        MAX(running_count_unit_price) OVER w_part AS last4_max_running_count_unit_price,
        AVG(running_count_unit_price) OVER w_part AS last4_avg_running_count_unit_price
    FROM base_order_items
    WINDOW w_part AS (ORDER BY unit_price ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    CASE WHEN quantity_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END AS quantity_rank_tier,
    PERCENTILE_APPROX(last4_max_running_count_unit_price, ARRAY(0.25, 0.5, 0.75)) AS last4_max_running_count_unit_price_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    COUNT(DISTINCT id) AS distinct_ids_3,
    ROUND(AVG(corr_unit_price_quantity), 2) AS avg_corr_unit_price_quantity,
    GROUPING_ID() AS grouping_id
FROM enriched_order_items
GROUP BY ROLLUP (CASE WHEN quantity_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END)
HAVING avg_corr_unit_price_quantity > 0
ORDER BY distinct_ids_3 DESC NULLS FIRST;
