WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue,
        COALESCE(units, 0) units_filled
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-06-21')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        sale_date,
        units,
        units_filled,
        MIN(units_filled) OVER w_part running_min_units_filled,
        AVG(units_filled) OVER w_part running_avg_units_filled,
        MAX(units_filled) OVER w_part running_max_units_filled
    FROM valid
    WINDOW w_part AS (PARTITION BY store_id ORDER BY units_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        sale_date,
        units,
        running_max_units_filled,
        LEAD(running_avg_units_filled) OVER (ORDER BY running_max_units_filled, id) next_running_avg_units_filled,
        LEAD(running_max_units_filled) OVER (ORDER BY running_max_units_filled, id) next_running_max_units_filled,
        ROW_NUMBER() OVER (ORDER BY running_max_units_filled, id) event_seq
    FROM with_windows
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(next_running_avg_units_filled) OVER (ORDER BY event_seq, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) next_running_avg_units_filled_avg_6_prior,
        AVG(next_running_avg_units_filled) OVER (ORDER BY sale_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) next_running_avg_units_filled_avg_2,
        LEAD(next_running_max_units_filled, 1, 0) OVER (ORDER BY units, id) next_next_running_max_units_filled
    FROM with_windows_store_sales
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
    CASE WHEN next_running_avg_units_filled_avg_6_prior >= 83 THEN 'LARGE' ELSE 'SMALL' END AS next_running_avg_units_filled_avg_6_prior_tier,
    CASE WHEN next_running_max_units_filled >= 81 THEN 'LARGE' ELSE 'SMALL' END AS next_running_max_units_filled_tier,
    COUNT(*) AS cnt,
    ROUND(AVG(next_running_avg_units_filled_avg_2), 2) AS avg_next_running_avg_units_filled_avg_2,
    COUNT(*) AS row_count
FROM enriched
GROUP BY DATE_TRUNC('QUARTER', sale_date), CASE WHEN next_running_avg_units_filled_avg_6_prior >= 83 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN next_running_max_units_filled >= 81 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY row_count DESC NULLS LAST;
