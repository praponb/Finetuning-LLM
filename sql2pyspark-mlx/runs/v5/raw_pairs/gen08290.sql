WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price + quantity AS line_total,
        unit_price + quantity AS line_total_2
    FROM order_items
    WHERE product_id IS NOT NULL
      AND product_id IN (3, 5, 29)
),
joined AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.order_id,
        s.product_id,
        s.quantity,
        s.unit_price,
        s.line_total,
        b.brand,
        b.cost,
        b.category
    FROM valid s
    LEFT JOIN products b
        ON s.product_id = b.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY line_total DESC NULLS FIRST, id) AS latest_rank
    FROM joined
    QUALIFY latest_rank = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT product_id
    FROM ranked
    EXCEPT DISTINCT
    SELECT product_id
    FROM order_items
    WHERE unit_price BETWEEN 130 AND 180
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    JOIN remaining_keys k ON s.product_id = k.product_id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_total,
        brand,
        latest_rank,
        ROUND(line_total / quantity, 2) AS line_total_per_quantity
    FROM verified
    WHERE quantity >= 4
),
valid_order_items AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(latest_rank * 0.85, 2) AS latest_rank_scaled,
        unit_price + line_total AS unit_price_plus_line_total
    FROM filtered
    WHERE line_total_per_quantity < 38
),
daily AS (
    -- Grouped metrics
    SELECT
        product_id,
        CASE WHEN line_total >= 74 THEN 'LARGE' ELSE 'SMALL' END AS line_total_tier,
        SUM(CASE WHEN brand = 'Globex' THEN line_total ELSE 0 END) AS globex_line_total,
        APPROX_PERCENTILE(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles,
        SIZE(COLLECT_SET(brand)) AS num_brands,
        SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
        SORT_ARRAY(COLLECT_SET(brand)) AS brand_list_2,
        ROUND(SUM(latest_rank_scaled) / NULLIF(COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END), 0), 2) AS latest_rank_scaled_per_umbrella
    FROM valid_order_items
    GROUP BY product_id, CASE WHEN line_total >= 74 THEN 'LARGE' ELSE 'SMALL' END
    HAVING ROUND(SUM(latest_rank_scaled) / NULLIF(COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END), 0), 2) > 0
)
SELECT
    s.product_id,
    s.line_total_tier,
    s.unit_price_quartiles,
    s.num_brands,
    s.brand_list,
    s.brand_list_2,
    s.latest_rank_scaled_per_umbrella,
    (SELECT AVG(o2.unit_price) FROM order_items o2 WHERE o2.product_id = s.product_id) AS avg_order_item_unit_price
FROM daily s;
