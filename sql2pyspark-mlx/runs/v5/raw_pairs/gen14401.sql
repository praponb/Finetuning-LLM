WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity + unit_price AS line_total
    FROM order_items
    WHERE order_id IN (4, 5, 6)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT product_id, order_id
    FROM valid
    INTERSECT ALL
    SELECT product_id, order_id
    FROM order_items
    WHERE order_id = 7
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.product_id = k.product_id AND s.order_id = k.order_id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        o.discount,
        o.shipping_fee,
        o.status
    FROM kept_rows a
    LEFT JOIN orders o
        ON a.order_id = o.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY shipping_fee) AS shipping_fee_rank,
        CORR(unit_price, quantity) OVER (PARTITION BY order_id, product_id) AS corr_unit_price_quantity
    FROM with_dims
    WHERE unit_price >= 250
),
enriched_order_items AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.order_id,
        src.product_id,
        src.unit_price,
        src.line_total,
        src.shipping_fee,
        p.launch_date,
        p.brand,
        p.cost
    FROM enriched src
    LEFT JOIN products p
        ON src.product_id <=> p.id
),
kept_keys_2 AS (
    -- Set intersection on keys
    SELECT id, product_id
    FROM enriched_order_items
    INTERSECT
    SELECT id, product_id
    FROM order_items
    WHERE quantity IS NULL
),
kept_rows_order_items AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_order_items s
    INNER JOIN kept_keys_2 k ON s.id = k.id AND s.product_id = k.product_id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        unit_price,
        line_total,
        shipping_fee,
        launch_date,
        brand,
        DENSE_RANK() OVER (ORDER BY line_total) AS line_total_dense_rank,
        AVG(shipping_fee) OVER (PARTITION BY order_id) AS order_id_avg_shipping_fee,
        COUNT(unit_price) OVER (PARTITION BY brand) AS brand_count_unit_price
    FROM kept_rows_order_items
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    COUNT(CASE WHEN brand = 'Acme' THEN 1 END) AS acme_count,
    PERCENTILE_APPROX(order_id_avg_shipping_fee, ARRAY(0.25, 0.5, 0.75)) AS order_id_avg_shipping_fee_quartiles,
    SUM(CASE WHEN brand = 'Initech' THEN unit_price ELSE 0 END) AS initech_unit_price,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    COUNT(*) AS event_count
FROM scored
GROUP BY quarter_start, week_start;
