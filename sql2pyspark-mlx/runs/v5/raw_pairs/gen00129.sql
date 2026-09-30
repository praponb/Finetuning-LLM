WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(quantity / unit_price, 2) AS line_total,
        FLOOR(quantity) AS quantity_whole
    FROM order_items
    WHERE unit_price >= 300
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        quantity,
        quantity_whole,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY quantity DESC, id ASC) AS quantity_bucket,
        LAG(quantity_whole, 1) OVER (PARTITION BY product_id, order_id ORDER BY quantity ASC, id ASC) AS prev_quantity_whole,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id), 4) AS unit_price_share,
        AVG(quantity_whole) OVER (PARTITION BY product_id ORDER BY quantity_whole ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS quantity_whole_avg_6
    FROM valid
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(quantity_whole) OVER (ORDER BY prev_quantity_whole ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_quantity_whole,
        COUNT(quantity_whole) OVER (ORDER BY prev_quantity_whole ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_quantity_whole,
        MIN(quantity_whole) OVER (ORDER BY prev_quantity_whole ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_quantity_whole,
        NTILE(3) OVER (ORDER BY quantity_whole_avg_6 DESC, id ASC) AS quantity_whole_avg_6_bucket
    FROM with_windows
    WHERE quantity_bucket <= 5
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        quantity,
        quantity_whole,
        quantity_bucket,
        unit_price_share,
        last3_max_quantity_whole,
        last3_count_quantity_whole,
        last3_min_quantity_whole,
        quantity_whole_avg_6_bucket,
        COALESCE(last3_max_quantity_whole, 0) AS last3_max_quantity_whole_filled
    FROM enriched
    WHERE quantity_whole_avg_6_bucket BETWEEN 6 AND 7
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(last3_min_quantity_whole) OVER (ORDER BY quantity_whole_avg_6_bucket ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_last3_min_quantity_whole,
        SUM(last3_min_quantity_whole) OVER (ORDER BY quantity_whole_avg_6_bucket ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_last3_min_quantity_whole,
        AVG(last3_min_quantity_whole) OVER (ORDER BY quantity_whole_avg_6_bucket ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_last3_min_quantity_whole,
        ROW_NUMBER() OVER (ORDER BY quantity DESC, id ASC) AS row_seq
    FROM cleaned
)
-- Grouped metrics
SELECT
    CASE WHEN quantity_bucket >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_bucket_tier,
    CASE WHEN last3_max_quantity_whole_filled >= 61 THEN 'LARGE' ELSE 'SMALL' END AS last3_max_quantity_whole_filled_tier,
    CASE WHEN last3_max_quantity_whole >= 86 THEN 'PREMIUM' ELSE 'STANDARD' END AS last3_max_quantity_whole_tier,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    ROUND(AVG(last7_count_last3_min_quantity_whole), 2) AS avg_last7_count_last3_min_quantity_whole,
    ROUND(AVG(last3_max_quantity_whole), 2) AS avg_last3_max_quantity_whole,
    COUNT(DISTINCT id) AS distinct_ids,
    BIT_AND(quantity) AS quantity_bit_and,
    COUNT(*) AS cnt
FROM metrics
GROUP BY CASE WHEN quantity_bucket >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN last3_max_quantity_whole_filled >= 61 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN last3_max_quantity_whole >= 86 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING avg_last3_max_quantity_whole < 1000000;
