WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        LEAST(quantity, unit_price) least_quantity_unit_price
    FROM order_items
    WHERE unit_price < 220
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY least_quantity_unit_price NULLS FIRST, id) AS dedup_rank
        FROM cleaned
    ) t
    WHERE dedup_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        dedup_rank,
        SUM(least_quantity_unit_price) OVER w_part running_sum_least_quantity_unit_price,
        AVG(least_quantity_unit_price) OVER w_part running_avg_least_quantity_unit_price,
        COUNT(least_quantity_unit_price) OVER w_part running_count_least_quantity_unit_price,
        MIN(least_quantity_unit_price) OVER w_part running_min_least_quantity_unit_price
    FROM latest
    WINDOW w_part AS (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.*,
        d.amount,
        d.discount,
        d.order_date
    FROM scored s
    LEFT JOIN orders d
        ON s.order_id = d.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', order_date) AS quarter_start,
    DATE_TRUNC('YEAR', order_date) AS year_start,
    CASE WHEN amount >= 1520 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    MIN(unit_price) AS min_unit_price,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(running_avg_least_quantity_unit_price, 0.9) AS running_avg_least_quantity_unit_price_p90,
    GROUPING(DATE_TRUNC('QUARTER', order_date)) AS grouping_level
FROM with_dims
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', order_date), DATE_TRUNC('YEAR', order_date), CASE WHEN amount >= 1520 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('QUARTER', order_date), DATE_TRUNC('YEAR', order_date)), (DATE_TRUNC('QUARTER', order_date)), ())
