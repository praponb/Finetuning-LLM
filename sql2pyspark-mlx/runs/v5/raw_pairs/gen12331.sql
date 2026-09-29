WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        opened_date,
        sqft,
        INITCAP(TRIM(region)) AS region_title,
        COALESCE(region, 'OTHER') AS region_filled
    FROM stores
    WHERE sqft > 11311
      AND opened_date > DATE '2023-09-24'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(sqft) OVER w_hist AS last7_max_sqft,
        MIN(sqft) OVER w_hist AS last7_min_sqft,
        COUNT(sqft) OVER w_hist AS last7_count_sqft,
        SUM(sqft) OVER w_hist AS last7_sum_sqft
    FROM valid
    WHERE store_name LIKE '%Kiosk%'
    WINDOW w_hist AS (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.order_date < s.opened_date
        )
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS row_num
    FROM kept
    QUALIFY row_num = 1
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN last7_count_sqft >= 17 THEN 'HIGH' ELSE 'LOW' END AS last7_count_sqft_tier,
        COUNT_IF(city = 'Chiang Mai') AS chiang_mai_events,
        SUM(CASE WHEN city = 'New York' THEN last7_count_sqft ELSE 0 END) AS new_york_last7_count_sqft
    FROM ranked
    GROUP BY CASE WHEN last7_count_sqft >= 17 THEN 'HIGH' ELSE 'LOW' END
    HAVING SUM(CASE WHEN city = 'New York' THEN last7_count_sqft ELSE 0 END) >= 0
)
SELECT
    last7_count_sqft_tier,
    chiang_mai_events,
    new_york_last7_count_sqft
FROM totals
ORDER BY new_york_last7_count_sqft DESC;
