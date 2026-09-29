WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC NULLS FIRST, id) AS dedup_rank
    FROM order_items
    QUALIFY dedup_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        dedup_rank,
        SUM(unit_price) OVER w_hist AS running_unit_price,
        AVG(unit_price) OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS unit_price_avg_5_prior,
        CORR(quantity, unit_price) OVER (PARTITION BY order_id, product_id) AS corr_quantity_unit_price
    FROM ranked
    WINDOW w_hist AS (PARTITION BY product_id, order_id ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        dedup_rank,
        running_unit_price,
        corr_quantity_unit_price,
        dedup_rank - running_unit_price AS dedup_rank_minus_running_unit_price,
        ABS(corr_quantity_unit_price) AS abs_corr_quantity_unit_price
    FROM scored
    WHERE corr_quantity_unit_price >= -0.3
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(dedup_rank_minus_running_unit_price) OVER w_grp AS product_id_sum_dedup_rank_minus_running_unit_price,
        NTILE(5) OVER w_running AS unit_price_bucket,
        SUM(abs_corr_quantity_unit_price) OVER w_grp_2 AS order_id_sum_abs_corr_quantity_unit_price,
        PERCENT_RANK() OVER (ORDER BY corr_quantity_unit_price DESC NULLS FIRST) AS corr_quantity_unit_price_percent_rank,
        SUM(dedup_rank_minus_running_unit_price) OVER (PARTITION BY order_id ORDER BY corr_quantity_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_dedup_rank_minus_running_unit_price
    FROM filtered
    WINDOW w_grp AS (PARTITION BY product_id), w_running AS (PARTITION BY order_id ORDER BY unit_price DESC, id), w_grp_2 AS (PARTITION BY order_id, product_id)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(abs_corr_quantity_unit_price) OVER (PARTITION BY product_id ORDER BY cumulative_dedup_rank_minus_running_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_abs_corr_quantity_unit_price,
        LAG(abs_corr_quantity_unit_price, 1) OVER (PARTITION BY order_id, product_id ORDER BY abs_corr_quantity_unit_price, id) AS prev_abs_corr_quantity_unit_price
    FROM with_windows
)
-- Grouped metrics
SELECT
    CASE WHEN corr_quantity_unit_price >= 0.77 THEN 'LARGE' ELSE 'SMALL' END AS corr_quantity_unit_price_tier,
    order_id,
    MAX(abs_corr_quantity_unit_price) AS max_abs_corr_quantity_unit_price,
    APPROX_PERCENTILE(abs_corr_quantity_unit_price, 0.95) AS abs_corr_quantity_unit_price_p95
FROM enriched
GROUP BY CASE WHEN corr_quantity_unit_price >= 0.77 THEN 'LARGE' ELSE 'SMALL' END, order_id
ORDER BY max_abs_corr_quantity_unit_price DESC NULLS LAST;
