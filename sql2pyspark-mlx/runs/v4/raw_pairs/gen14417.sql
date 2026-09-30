WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN unit_price >= 350 THEN 'High' WHEN unit_price >= 150 THEN 'Medium' ELSE 'Low' END unit_price_level,
        unit_price - quantity line_total
    FROM order_items
    WHERE quantity BETWEEN 4 AND 5
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price_level,
        line_total,
        SUM(line_total) OVER (ORDER BY line_total, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_line_total,
        SUM(unit_price) OVER (PARTITION BY order_id, product_id ORDER BY line_total, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_unit_price,
        SUM(line_total) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_line_total_2,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY unit_price DESC) unit_price_percent_rank,
        AVG(line_total) OVER (PARTITION BY product_id, order_id ORDER BY quantity, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) line_total_avg_5_prior
    FROM base
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(cumulative_line_total_2) OVER w_hist cumulative_line_total_2_avg_5_prior,
        NTILE(10) OVER (ORDER BY line_total_avg_5_prior DESC, id) line_total_avg_5_prior_bucket,
        COUNT(line_total) OVER (PARTITION BY order_id, product_id) order_id_count_line_total,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY cumulative_line_total DESC NULLS LAST) cumulative_line_total_cume_dist
    FROM scored
    WINDOW w_hist AS (ORDER BY cumulative_line_total_2, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY cumulative_line_total, id) AS rn
        FROM with_windows
    ) x
    WHERE rn = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.amount,
        d.status,
        d.order_date
    FROM latest e
    LEFT JOIN orders d
        ON e.order_id = d.id
)
SELECT
    id,
    order_id,
    product_id,
    quantity,
    unit_price_level,
    line_total,
    cumulative_unit_price,
    unit_price_percent_rank,
    line_total_avg_5_prior
FROM enriched
ORDER BY product_id DESC;
