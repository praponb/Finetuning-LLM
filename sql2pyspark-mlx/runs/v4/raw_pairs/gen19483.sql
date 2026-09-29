WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        CAST(unit_price AS INT) AS unit_price_int,
        quantity - unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND product_id IS NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) = 1
),
ranked_order_items AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY quantity DESC, id) AS latest_rank
    FROM ranked
    QUALIFY latest_rank = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, order_id
    FROM ranked_order_items
    EXCEPT DISTINCT
    SELECT id, order_id
    FROM order_items
    WHERE order_id IN (15, 24, 25)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked_order_items s
    JOIN kept_keys k ON s.id = k.id AND s.order_id = k.order_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price_int,
        line_total,
        latest_rank,
        MAX(unit_price) OVER (PARTITION BY order_id) AS order_id_max_unit_price,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST) AS quantity_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY quantity NULLS LAST) AS quantity_percent_rank_2,
        CORR(unit_price, quantity) OVER (PARTITION BY product_id) AS corr_unit_price_quantity,
        MIN(unit_price) OVER (PARTITION BY product_id, order_id) AS product_id_min_unit_price
    FROM reconciled
),
enriched AS (
    -- Attach orders attributes
    SELECT e.*, o.channel
    FROM metrics e
    JOIN orders o
        ON e.order_id = o.id
)
-- Grouped metrics
SELECT
    CASE WHEN unit_price_int >= 70 THEN 'PREMIUM' ELSE 'STANDARD' END AS unit_price_int_tier,
    CASE WHEN product_id_min_unit_price >= 428 THEN 'HIGH' ELSE 'LOW' END AS product_id_min_unit_price_tier,
    CASE WHEN quantity >= 8 THEN 'HIGH' ELSE 'LOW' END AS quantity_tier,
    COUNT_IF(channel = 'store') AS store_events,
    COUNT_IF(channel = 'mobile') AS mobile_events,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list
FROM enriched
GROUP BY CASE WHEN unit_price_int >= 70 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN product_id_min_unit_price >= 428 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN quantity >= 8 THEN 'HIGH' ELSE 'LOW' END
ORDER BY unit_price_int_tier NULLS LAST
