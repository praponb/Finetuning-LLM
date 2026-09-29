WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(unit_price) AS unit_price_whole
    FROM order_items
    WHERE quantity <= 8
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        unit_price_whole,
        unit_price_whole * unit_price AS unit_price_whole_times_unit_price,
        unit_price / NULLIF(unit_price_whole, 0) AS unit_price_per_unit_price_whole
    FROM cleaned
    WHERE product_id IS NOT NULL
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, product_id
    FROM valid
    INTERSECT
    SELECT id, product_id
    FROM order_items
    WHERE quantity < 7
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.id = k.id AND s.product_id = k.product_id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST) AS quantity_cume_dist,
        SUM(unit_price_per_unit_price_whole) OVER (PARTITION BY product_id) AS product_id_sum_unit_price_per_unit_price_whole,
        RANK() OVER (PARTITION BY product_id ORDER BY unit_price_per_unit_price_whole DESC) AS unit_price_per_unit_price_whole_rank
    FROM kept_rows
),
kept_keys_2 AS (
    -- Set intersection on keys
    SELECT product_id, id
    FROM with_windows
    INTERSECT ALL
    SELECT product_id, id
    FROM order_items
    WHERE unit_price BETWEEN 220 AND 390
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys_2 k ON s.product_id = k.product_id AND s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY unit_price NULLS FIRST) AS unit_price_cume_dist,
        COUNT(unit_price_whole_times_unit_price) OVER (PARTITION BY product_id) AS product_id_count_unit_price_whole_times_unit_price
    FROM verified
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(unit_price_per_unit_price_whole_rank / SUM(unit_price_per_unit_price_whole_rank) OVER (PARTITION BY product_id), 4) AS unit_price_per_unit_price_whole_rank_share,
        RANK() OVER w_part AS quantity_cume_dist_rank
    FROM enriched
    WINDOW w_part AS (PARTITION BY product_id ORDER BY quantity_cume_dist NULLS LAST)
),
grouped AS (
    -- Grouped metrics
    SELECT
        product_id,
        PERCENTILE_APPROX(unit_price_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS unit_price_cume_dist_quartiles,
        BIT_OR(quantity) AS quantity_bit_or,
        APPROX_PERCENTILE(quantity_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS quantity_cume_dist_quartiles,
        APPROX_PERCENTILE(unit_price_whole, 0.5) AS unit_price_whole_p50,
        APPROX_PERCENTILE(product_id_sum_unit_price_per_unit_price_whole, ARRAY(0.25, 0.5, 0.75)) AS product_id_sum_unit_price_per_unit_price_whole_quartiles,
        AVG(quantity_cume_dist) AS avg_quantity_cume_dist
    FROM metrics
    GROUP BY product_id
    HAVING AVG(quantity_cume_dist) >= 0
)
SELECT
    product_id,
    unit_price_cume_dist_quartiles,
    quantity_bit_or,
    quantity_cume_dist_quartiles,
    unit_price_whole_p50,
    avg_quantity_cume_dist
FROM grouped
ORDER BY avg_quantity_cume_dist NULLS LAST, quantity_bit_or
