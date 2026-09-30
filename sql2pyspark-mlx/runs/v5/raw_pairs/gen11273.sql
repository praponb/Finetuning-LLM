WITH enriched AS (
    SELECT
        *,
        FIRST_VALUE(sqft, TRUE) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) AS row_seq
    FROM stores
    WHERE store_name IS NOT NULL
),
enriched_stores AS (
    SELECT
        id,
        city,
        region,
        sqft,
        row_seq,
        FIRST_VALUE(sqft, TRUE) OVER (PARTITION BY city ORDER BY first_known_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft_2,
        CUME_DIST() OVER (PARTITION BY city ORDER BY first_known_sqft NULLS LAST) AS first_known_sqft_cume_dist
    FROM enriched
),
with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY sqft DESC NULLS FIRST) AS sqft_cume_dist,
        NTILE(10) OVER (PARTITION BY region ORDER BY sqft, id) AS sqft_bucket,
        NTILE(10) OVER (PARTITION BY city ORDER BY row_seq, id) AS row_seq_bucket
    FROM enriched_stores
),
metrics AS (
    SELECT
        id,
        region,
        row_seq,
        first_known_sqft_2,
        sqft_cume_dist,
        row_seq_bucket,
        SUM(sqft) OVER w_hist AS running_sqft,
        LAST_VALUE(first_known_sqft_2) IGNORE NULLS OVER (PARTITION BY region, city ORDER BY sqft_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_known_sqft_2,
        NTILE(5) OVER (PARTITION BY city ORDER BY sqft_bucket DESC, id) AS sqft_bucket_bucket,
        SUM(sqft_bucket) OVER w_hist_2 AS cumulative_sqft_bucket
    FROM with_windows
    WINDOW w_hist AS (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist_2 AS (ORDER BY sqft_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND YEAR(ch.sale_date) = 2025
        )
),
with_latest AS (
    SELECT
        s.*,
        x.store_sale_revenue,
        x.store_sale_sale_date
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue, c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
stats AS (
    SELECT
        DATE_TRUNC('MONTH', store_sale_sale_date) AS activity_month,
        DATE_TRUNC('QUARTER', store_sale_sale_date) AS quarter_start,
        region,
        MAX(row_seq) FILTER (WHERE region = 'East') AS east_max_row_seq,
        COUNT(CASE WHEN region = 'East' THEN 1 END) AS east_count,
        COUNT_IF(region = 'North') AS north_events,
        SUM(CASE WHEN region = 'Central' THEN row_seq ELSE 0 END) AS central_row_seq,
        COUNT(DISTINCT id) AS distinct_ids,
        GROUPING_ID() AS gid
    FROM with_latest
    GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', store_sale_sale_date), DATE_TRUNC('QUARTER', store_sale_sale_date), region), (DATE_TRUNC('MONTH', store_sale_sale_date), DATE_TRUNC('QUARTER', store_sale_sale_date)), (DATE_TRUNC('MONTH', store_sale_sale_date)))
    HAVING COUNT_IF(region = 'North') > 1
)
SELECT
    activity_month,
    quarter_start,
    region,
    central_row_seq
FROM stats
