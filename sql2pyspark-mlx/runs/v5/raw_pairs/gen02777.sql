WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id, order_id
    FROM order_items
    EXCEPT
    SELECT id, order_id
    FROM order_items
    WHERE quantity < 3
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM order_items s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.order_id = k.order_id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS quantity_dense_rank,
        COUNT(unit_price) OVER (PARTITION BY product_id) AS product_id_count_unit_price,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY order_id), 4) AS unit_price_share,
        RANK() OVER (ORDER BY quantity DESC) AS quantity_rank
    FROM verified
    WHERE order_id IN (15, 23, 35, 40)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(quantity_rank, 0) AS quantity_rank_filled,
        ROUND(unit_price_share * 1.15, 2) AS adjusted_unit_price_share
    FROM scored
    WHERE product_id_count_unit_price BETWEEN 7 AND 9
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        quantity_dense_rank,
        product_id_count_unit_price,
        unit_price_share,
        quantity_rank,
        quantity_rank_filled,
        COALESCE(quantity_rank_filled, 0) AS quantity_rank_filled_filled
    FROM filtered
    WHERE order_id = 7
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity,
        quantity_rank_filled,
        quantity_rank_filled_filled,
        ROUND(quantity_rank / SUM(quantity_rank) OVER (PARTITION BY product_id, order_id), 4) AS quantity_rank_share,
        COUNT(quantity_rank_filled_filled) OVER (PARTITION BY product_id) AS product_id_count_quantity_rank_filled_filled,
        MIN(quantity_rank_filled_filled) OVER (PARTITION BY order_id) AS order_id_min_quantity_rank_filled_filled
    FROM cleaned
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT *, PERCENT_RANK() OVER (ORDER BY quantity_rank_share NULLS FIRST) AS quantity_rank_share_percent_rank
    FROM metrics
)
-- Multi-level aggregation
SELECT
    CASE WHEN quantity >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    CASE WHEN quantity_rank_share >= 0.65 THEN 'HIGH' ELSE 'LOW' END AS quantity_rank_share_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    BIT_AND(quantity) AS quantity_bit_and,
    COUNT(DISTINCT id) AS distinct_ids_2,
    RANK() OVER (PARTITION BY CASE WHEN quantity >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_2_rank
FROM enriched
GROUP BY CASE WHEN quantity >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN quantity_rank_share >= 0.65 THEN 'HIGH' ELSE 'LOW' END;
