WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        quantity,
        unit_price,
        ROUND(quantity / unit_price, 2) AS line_total
    FROM order_items
    WHERE quantity <> 4
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY quantity, id) AS seq_no,
        NTILE(10) OVER (ORDER BY line_total DESC, id) AS line_total_bucket
    FROM base
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        unit_price,
        line_total,
        seq_no,
        line_total_bucket,
        DENSE_RANK() OVER (ORDER BY seq_no NULLS LAST) AS seq_no_dense_rank,
        CUME_DIST() OVER (ORDER BY line_total DESC NULLS FIRST) AS line_total_cume_dist
    FROM with_windows
),
metrics_order_items AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (ORDER BY seq_no_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        DENSE_RANK() OVER (ORDER BY quantity DESC NULLS LAST) AS quantity_dense_rank,
        RANK() OVER (ORDER BY unit_price DESC) AS unit_price_rank
    FROM metrics
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        quantity,
        unit_price,
        line_total,
        line_total_bucket,
        seq_no_dense_rank,
        line_total_cume_dist,
        quantity_dense_rank,
        unit_price_rank,
        quantity_dense_rank + quantity AS quantity_dense_rank_plus_quantity
    FROM metrics_order_items
    WHERE unit_price_rank BETWEEN 4 AND 5
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN seq_no_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS seq_no_dense_rank_tier,
        BIT_AND(quantity) AS quantity_bit_and,
        ROUND(AVG(quantity), 2) AS avg_quantity,
        COUNT(*) AS cnt
    FROM valid
    GROUP BY CASE WHEN seq_no_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END
)
SELECT seq_no_dense_rank_tier, quantity_bit_and
FROM daily
ORDER BY seq_no_dense_rank_tier DESC NULLS LAST
