WITH base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(unit_price, 0) AS unit_price_rounded
    FROM order_items
    WHERE quantity BETWEEN 4 AND 5
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.brand,
        p.product_name
    FROM base a
    LEFT JOIN products p
        ON a.product_id = p.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price_rounded DESC NULLS FIRST, id) AS row_num
    FROM with_dims
    QUALIFY row_num = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price_rounded,
        brand,
        product_name,
        row_num,
        LEAST(row_num, unit_price_rounded) AS least_row_num_unit_price_rounded
    FROM deduped
    WHERE order_id IN (16, 31)
),
enriched AS (
    -- Attach products attributes
    SELECT s.*, p.brand AS product_brand
    FROM valid s
    INNER JOIN products p
        ON s.product_id <=> p.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(least_row_num_unit_price_rounded) OVER (PARTITION BY product_id ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_least_row_num_unit_price_rounded,
        SUM(least_row_num_unit_price_rounded) OVER (PARTITION BY product_id ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_least_row_num_unit_price_rounded,
        COUNT(least_row_num_unit_price_rounded) OVER (PARTITION BY product_id ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_least_row_num_unit_price_rounded
    FROM enriched
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price_rounded,
        brand,
        product_name,
        row_num,
        running_max_least_row_num_unit_price_rounded,
        running_count_least_row_num_unit_price_rounded,
        MAX(running_sum_least_row_num_unit_price_rounded) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_running_sum_least_row_num_unit_price_rounded,
        COUNT(running_sum_least_row_num_unit_price_rounded) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_running_sum_least_row_num_unit_price_rounded,
        AVG(running_sum_least_row_num_unit_price_rounded) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_running_sum_least_row_num_unit_price_rounded,
        SUM(running_sum_least_row_num_unit_price_rounded) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_sum_least_row_num_unit_price_rounded
    FROM scored
)
-- Join a dimension and aggregate
SELECT
    d.brand AS product_brand,
    src.brand AS order_item_brand,
    COUNT(*) AS cnt,
    ROUND(AVG(src.running_sum_running_sum_least_row_num_unit_price_rounded), 2) AS avg_running_sum_running_sum_least_row_num_unit_price_rounded,
    AVG(src.running_count_least_row_num_unit_price_rounded) AS avg_running_count_least_row_num_unit_price_rounded,
    GROUPING_ID() AS grouping_level
FROM metrics src
LEFT JOIN products d ON src.product_id = d.id
GROUP BY GROUPING SETS ((d.brand, src.brand), (d.brand), ())
ORDER BY cnt NULLS LAST
