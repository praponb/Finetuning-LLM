WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
    FROM store_sales
    WHERE units < 28
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        days_since_sale_date,
        SUM(days_since_sale_date) OVER w_ordered AS cumulative_days_since_sale_date,
        SUM(days_since_sale_date) OVER w_grp AS cumulative_days_since_sale_date_2,
        LAST_VALUE(days_since_sale_date, TRUE) OVER w_ordered_2 AS last_known_days_since_sale_date,
        MIN(days_since_sale_date) OVER w_running AS store_id_min_days_since_sale_date
    FROM filtered
    WINDOW w_ordered AS (ORDER BY days_since_sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY store_id ORDER BY days_since_sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered_2 AS (ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY store_id, product_id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(cumulative_days_since_sale_date) OVER w_ordered AS store_id_min_cumulative_days_since_sale_date,
        MAX(last_known_days_since_sale_date) OVER (PARTITION BY product_id, store_id) AS product_id_max_last_known_days_since_sale_date,
        LAST_VALUE(cumulative_days_since_sale_date_2) IGNORE NULLS OVER w_running AS last_known_cumulative_days_since_sale_date_2,
        SUM(cumulative_days_since_sale_date) OVER w_running AS cumulative_cumulative_days_since_sale_date
    FROM scored
    WINDOW w_ordered AS (PARTITION BY store_id, product_id), w_running AS (ORDER BY last_known_days_since_sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT product_id, id
    FROM with_windows
    INTERSECT ALL
    SELECT product_id, id
    FROM store_sales
    WHERE revenue BETWEEN 2000 AND 4000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN kept_keys k ON s.product_id = k.product_id AND s.id = k.id
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
        store_id,
        PERCENTILE_APPROX(cumulative_days_since_sale_date_2, 0.5) AS cumulative_days_since_sale_date_2_p50,
        ROUND(AVG(cumulative_cumulative_days_since_sale_date), 2) AS avg_cumulative_cumulative_days_since_sale_date,
        ROUND(AVG(product_id_max_last_known_days_since_sale_date), 2) AS avg_product_id_max_last_known_days_since_sale_date,
        COUNT(DISTINCT id) AS distinct_ids,
        GROUPING(DATE_TRUNC('QUARTER', sale_date)) AS gid
    FROM kept_rows
    GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', sale_date), store_id), (DATE_TRUNC('QUARTER', sale_date)))
    HAVING distinct_ids >= 1
)
SELECT
    quarter_start,
    store_id,
    cumulative_days_since_sale_date_2_p50,
    avg_cumulative_cumulative_days_since_sale_date,
    avg_product_id_max_last_known_days_since_sale_date,
    distinct_ids
FROM grouped
ORDER BY avg_product_id_max_last_known_days_since_sale_date DESC, distinct_ids DESC;
