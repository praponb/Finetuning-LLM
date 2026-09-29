WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, quantity - unit_price AS line_total
    FROM order_items
    WHERE quantity > 3
),
valid_order_items AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        line_total,
        FLOOR(unit_price) AS unit_price_whole
    FROM valid
    WHERE product_id IN (9, 28, 30, 34)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY unit_price DESC NULLS FIRST, id) AS dedup_rank
    FROM valid_order_items
    QUALIFY dedup_rank = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY line_total NULLS FIRST, id) AS rn
        FROM latest
    ) x
    WHERE rn = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        line_total,
        rn,
        SUM(unit_price_whole) OVER (ORDER BY line_total, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price_whole,
        SUM(line_total) OVER (PARTITION BY product_id, order_id ORDER BY unit_price_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_line_total,
        AVG(line_total) OVER w_hist AS line_total_avg_2,
        MIN(line_total) OVER (PARTITION BY product_id) AS product_id_min_line_total
    FROM ranked
    WINDOW w_hist AS (ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY rn DESC, id) = 1
)
-- Grouped metrics
SELECT
    order_id,
    product_id,
    CASE WHEN product_id_min_line_total >= 90 THEN 'PREMIUM' ELSE 'STANDARD' END AS product_id_min_line_total_tier,
    APPROX_PERCENTILE(cumulative_line_total, ARRAY(0.25, 0.5, 0.75)) AS cumulative_line_total_quartiles,
    APPROX_PERCENTILE(running_unit_price_whole, 0.5) AS running_unit_price_whole_p50,
    APPROX_PERCENTILE(product_id_min_line_total, ARRAY(0.25, 0.5, 0.75)) AS product_id_min_line_total_quartiles,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    PERCENTILE_APPROX(line_total_avg_2, 0.5) AS line_total_avg_2_p50,
    COUNT(DISTINCT order_id) AS distinct_order_ids
FROM deduped
GROUP BY order_id, product_id, CASE WHEN product_id_min_line_total >= 90 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY product_id DESC NULLS LAST;
