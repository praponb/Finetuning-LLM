WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, LAST_DAY(last_restock) AS last_restock_month_end
    FROM inventory
    WHERE store_id = 4
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        store_id,
        quantity,
        last_restock_month_end
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_month_end DESC NULLS LAST, id) AS latest_rank
        FROM valid
    ) t
    WHERE latest_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        MIN(quantity) OVER w_running AS last7_min_quantity,
        MAX(quantity) OVER w_running AS last7_max_quantity,
        COUNT(quantity) OVER w_running AS last7_count_quantity,
        NTILE(4) OVER (PARTITION BY store_id ORDER BY quantity, id) AS quantity_bucket
    FROM ranked
    WINDOW w_running AS (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity_bucket DESC, id) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
)
-- Grouped metrics
SELECT
    store_id,
    PERCENTILE_APPROX(last7_count_quantity, ARRAY(0.25, 0.5, 0.75)) AS last7_count_quantity_quartiles,
    PERCENTILE_APPROX(last7_count_quantity, ARRAY(0.25, 0.5, 0.75)) AS last7_count_quantity_quartiles_2,
    PERCENTILE_APPROX(quantity_bucket, 0.9) AS quantity_bucket_p90,
    MIN(last7_count_quantity) AS min_last7_count_quantity,
    APPROX_PERCENTILE(last7_min_quantity, 0.9) AS last7_min_quantity_p90
FROM latest
GROUP BY 1
HAVING min_last7_count_quantity < 1000000
ORDER BY quantity_bucket_p90 DESC NULLS FIRST;
