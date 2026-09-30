WITH base AS (
    SELECT *, COALESCE(store_name, 'N/A') store_name_filled
    FROM stores
    WHERE opened_date > DATE '2023-04-15'
      AND MONTH(opened_date) IN (2, 6, 11)
),
metrics AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        store_name_filled,
        PERCENT_RANK() OVER (ORDER BY sqft NULLS LAST) sqft_percent_rank,
        FIRST_VALUE(region) IGNORE NULLS OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_region,
        LAG(sqft, 2) OVER (ORDER BY sqft, id) prev_sqft
    FROM base
),
metrics_stores AS (
    SELECT
        *,
        LAG(sqft_percent_rank, 1) OVER w_grp prev_sqft_percent_rank,
        LAST_VALUE(prev_sqft, TRUE) OVER (PARTITION BY city ORDER BY sqft_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_prev_sqft,
        AVG(sqft) OVER (PARTITION BY region ORDER BY prev_sqft, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) sqft_avg_5_prior
    FROM metrics
    WINDOW w_grp AS (PARTITION BY city, region ORDER BY sqft, id)
),
metrics_stores_2 AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        store_name_filled,
        sqft_percent_rank,
        first_known_region,
        prev_sqft,
        prev_sqft_percent_rank,
        last_known_prev_sqft,
        sqft_avg_5_prior,
        LAG(sqft_avg_5_prior, 1) OVER (PARTITION BY city, region ORDER BY sqft, id) prev_sqft_avg_5_prior,
        RANK() OVER (PARTITION BY region ORDER BY last_known_prev_sqft DESC NULLS FIRST) last_known_prev_sqft_rank,
        LAST_VALUE(sqft_percent_rank) IGNORE NULLS OVER (PARTITION BY city ORDER BY prev_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_sqft_percent_rank
    FROM metrics_stores
),
enriched AS (
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY region ORDER BY prev_sqft DESC, id) prev_sqft_bucket,
        LAST_VALUE(sqft) IGNORE NULLS OVER w_recent last_known_sqft,
        PERCENT_RANK() OVER (PARTITION BY region ORDER BY prev_sqft_percent_rank NULLS LAST) prev_sqft_percent_rank_percent_rank,
        NTILE(10) OVER (PARTITION BY city, region ORDER BY last_known_prev_sqft_rank, id) last_known_prev_sqft_rank_bucket,
        SUM(sqft) OVER (PARTITION BY region ORDER BY sqft_avg_5_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_sqft
    FROM metrics_stores_2
    WINDOW w_recent AS (ORDER BY sqft_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date >= s.opened_date
        )
),
totals AS (
    SELECT
        DATE_TRUNC('YEAR', opened_date) AS year_start,
        DATE_TRUNC('MONTH', opened_date) AS activity_month,
        COUNT(*) AS cnt,
        COUNT(CASE WHEN city = 'Bangkok' THEN 1 END) AS bangkok_count,
        MAX(last_known_sqft) FILTER (WHERE region = 'North') AS north_max_last_known_sqft
    FROM eligible
    GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', opened_date), DATE_TRUNC('MONTH', opened_date)), (DATE_TRUNC('YEAR', opened_date)), ())
)
SELECT
    year_start,
    activity_month,
    cnt,
    bangkok_count,
    north_max_last_known_sqft
FROM totals;
