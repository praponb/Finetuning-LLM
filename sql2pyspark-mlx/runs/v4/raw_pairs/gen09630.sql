WITH cleaned AS (
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        CONCAT_WS('-', region, store_name) AS region_store_name_key,
        CAST(sqft AS DOUBLE) AS sqft_num
    FROM stores
    WHERE store_name NOT LIKE 'O%'
),
deduped AS (
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft_num DESC, id) = 1
),
filtered AS (
    SELECT
        *,
        DATE_TRUNC('WEEK', opened_date) AS opened_date_period,
        COALESCE(region, 'N/A') AS resolved_region
    FROM deduped
    WHERE opened_date > TO_DATE('2023-12-26')
      AND region IS NOT NULL
      AND sqft_num >= 27
),
metrics AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft_num DESC NULLS LAST) AS sqft_num_dense_rank,
        SUM(sqft) OVER (ORDER BY opened_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        LAG(sqft, 2) OVER (PARTITION BY region ORDER BY sqft, id) AS prev_sqft,
        NTILE(4) OVER (PARTITION BY region ORDER BY sqft_num, id) AS sqft_num_bucket,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft_num DESC NULLS FIRST) AS sqft_num_dense_rank_2
    FROM filtered
),
filtered_stores AS (
    SELECT *, sqft_num - sqft_num_bucket AS sqft_num_minus_sqft_num_bucket
    FROM metrics
    WHERE region IS NOT NULL
),
eligible AS (
    SELECT s.*
    FROM filtered_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.revenue IS NOT NULL
        )
),
metrics_stores AS (
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        region_store_name_key,
        sqft_num,
        opened_date_period,
        resolved_region,
        sqft_num_dense_rank,
        cumulative_sqft,
        prev_sqft,
        sqft_num_bucket,
        sqft_num_dense_rank_2,
        sqft_num_minus_sqft_num_bucket,
        AVG(prev_sqft) OVER (ORDER BY CAST(opened_date_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS prev_sqft_avg_30d,
        PERCENT_RANK() OVER (PARTITION BY region ORDER BY cumulative_sqft DESC NULLS FIRST) AS cumulative_sqft_percent_rank,
        LAG(sqft_num_dense_rank_2, 1) OVER (PARTITION BY region ORDER BY opened_date, id) AS prev_sqft_num_dense_rank_2
    FROM eligible
)
SELECT
    id,
    store_name,
    region,
    sqft,
    region_store_name_key,
    sqft_num,
    opened_date_period,
    cumulative_sqft,
    prev_sqft
FROM metrics_stores
ORDER BY opened_date_period DESC;
