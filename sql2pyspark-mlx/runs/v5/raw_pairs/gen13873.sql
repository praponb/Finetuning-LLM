WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        REGEXP_REPLACE(store_name, ' ', '') AS store_name_slug,
        COALESCE(region, 'UNKNOWN') AS region_filled
    FROM stores
    WHERE sqft > 11901
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_sqft,
        MIN(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_sqft,
        MAX(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_sqft,
        COUNT(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_sqft,
        AVG(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sqft_avg_6
    FROM cleaned
),
scored_stores AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        opened_date,
        store_name_slug,
        running_min_sqft,
        running_max_sqft,
        running_count_sqft,
        LAG(running_count_sqft, 2, 0) OVER (ORDER BY running_count_sqft, id) AS prev_running_count_sqft,
        LEAD(sqft_avg_6, 1, 0) OVER (PARTITION BY city, region ORDER BY opened_date, id) AS next_sqft_avg_6,
        AVG(running_max_sqft) OVER w_running AS running_max_sqft_avg_2,
        LEAD(running_min_sqft, 1) OVER (PARTITION BY region ORDER BY sqft_avg_6, id) AS next_running_min_sqft,
        FIRST_VALUE(running_count_sqft, TRUE) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_count_sqft
    FROM scored
    WHERE sqft_avg_6 >= 9971
    WINDOW w_running AS (ORDER BY running_max_sqft, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        opened_date,
        store_name_slug,
        running_min_sqft,
        running_max_sqft,
        next_sqft_avg_6,
        running_max_sqft_avg_2,
        COUNT(prev_running_count_sqft) OVER w_grp AS last3_count_prev_running_count_sqft,
        MIN(prev_running_count_sqft) OVER w_grp AS last3_min_prev_running_count_sqft,
        AVG(prev_running_count_sqft) OVER w_grp AS last3_avg_prev_running_count_sqft,
        SUM(prev_running_count_sqft) OVER w_grp AS last3_sum_prev_running_count_sqft
    FROM scored_stores
    WINDOW w_grp AS (PARTITION BY city ORDER BY next_sqft_avg_6, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.opened_date,
        s.running_min_sqft,
        s.running_max_sqft,
        s.running_max_sqft_avg_2,
        s.last3_min_prev_running_count_sqft,
        top1.inventory_quantity
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN inventory_quantity >= 208 THEN 'HIGH' ELSE 'LOW' END AS inventory_quantity_tier,
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    APPROX_PERCENTILE(running_min_sqft, ARRAY(0.25, 0.5, 0.75)) AS running_min_sqft_quartiles,
    PERCENTILE_APPROX(running_min_sqft, ARRAY(0.25, 0.5, 0.75)) AS running_min_sqft_quartiles_2,
    ROUND(AVG(running_min_sqft), 2) AS avg_running_min_sqft,
    APPROX_PERCENTILE(running_min_sqft, 0.5) AS running_min_sqft_p50,
    COUNT(*) AS num_rows,
    GROUPING(CASE WHEN inventory_quantity >= 208 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM with_latest
GROUP BY ROLLUP (CASE WHEN inventory_quantity >= 208 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', opened_date))
HAVING num_rows >= 1;
