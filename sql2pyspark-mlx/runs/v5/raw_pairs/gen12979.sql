WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(unit_price, 0) AS unit_price_filled
    FROM order_items
    WHERE product_id IN (3, 4, 27, 31)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS rank_in_group
        FROM cleaned
    ) ranked
    WHERE rank_in_group = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT product_id, order_id
    FROM ranked
    EXCEPT
    SELECT product_id, order_id
    FROM order_items
    WHERE product_id IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.product_id = k.product_id AND s.order_id = k.order_id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER w_running AS unit_price_filled_rank,
        AVG(unit_price) OVER (PARTITION BY product_id) AS product_id_avg_unit_price
    FROM kept_rows
    WHERE order_id IN (26, 33)
    WINDOW w_running AS (PARTITION BY order_id ORDER BY unit_price_filled)
),
scored AS (
    -- Running and moving aggregates
    SELECT *, RANK() OVER (PARTITION BY order_id, product_id ORDER BY unit_price NULLS FIRST) AS unit_price_rank
    FROM metrics
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        unit_price_filled,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id), 4) AS unit_price_share,
        ROUND(product_id_avg_unit_price / SUM(product_id_avg_unit_price) OVER (PARTITION BY order_id), 4) AS product_id_avg_unit_price_share,
        SUM(unit_price_filled_rank) OVER (PARTITION BY order_id) AS order_id_sum_unit_price_filled_rank
    FROM scored
),
joined AS (
    -- Attach products attributes
    SELECT
        src.product_id,
        src.product_id_avg_unit_price_share,
        src.order_id_sum_unit_price_filled_rank,
        b.brand,
        b.stock_qty
    FROM with_windows src
    LEFT JOIN products b
        ON src.product_id = b.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN product_id_avg_unit_price_share >= 0.77 THEN 'HIGH' ELSE 'LOW' END AS product_id_avg_unit_price_share_tier,
    PERCENTILE_APPROX(product_id_avg_unit_price_share, ARRAY(0.25, 0.5, 0.75)) AS product_id_avg_unit_price_share_quartiles,
    COUNT_IF(brand = 'Initech') AS initech_events,
    ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN brand = 'Globex' THEN 1 END), 0), 2) AS stock_qty_per_globex,
    ROUND(SUM(order_id_sum_unit_price_filled_rank) / NULLIF(COUNT(CASE WHEN brand = 'Stark' THEN 1 END), 0), 2) AS order_id_sum_unit_price_filled_rank_per_stark,
    SIZE(COLLECT_SET(brand)) AS num_brands
FROM joined
GROUP BY 1
