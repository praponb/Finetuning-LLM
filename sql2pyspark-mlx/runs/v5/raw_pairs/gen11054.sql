WITH filtered AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        CONCAT(city, ' - ', region) AS city_region,
        COALESCE(region, 'N/A') AS region_filled
    FROM stores
    WHERE opened_date > TO_DATE('2023-09-03')
),
metrics AS (
    SELECT
        id,
        region,
        city_region,
        region_filled,
        COUNT(sqft) OVER (PARTITION BY region) AS region_count_sqft,
        AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS sqft_avg_3,
        SUM(sqft) OVER (PARTITION BY city) AS city_sum_sqft,
        AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sqft_avg_6_prior
    FROM filtered
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY city_sum_sqft, id) AS row_num
        FROM metrics
    ) t
    WHERE row_num = 1
),
metrics_stores AS (
    SELECT
        id,
        region,
        region_filled,
        sqft_avg_3,
        row_num,
        LEAD(city_sum_sqft) OVER (PARTITION BY region ORDER BY row_num, id) AS next_city_sum_sqft,
        LAG(city_sum_sqft) OVER (PARTITION BY region ORDER BY row_num, id) AS prev_city_sum_sqft,
        LAG(sqft_avg_3) OVER (PARTITION BY region ORDER BY row_num, id) AS prev_sqft_avg_3,
        LEAD(sqft_avg_3) OVER (PARTITION BY region ORDER BY row_num, id) AS next_sqft_avg_3,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY row_num, id) AS row_seq,
        ROUND(sqft_avg_6_prior / SUM(sqft_avg_6_prior) OVER (PARTITION BY region), 4) AS sqft_avg_6_prior_share
    FROM deduped
),
enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY sqft_avg_6_prior_share) AS sqft_avg_6_prior_share_dense_rank,
        ROUND(sqft_avg_3 / SUM(sqft_avg_3) OVER (PARTITION BY region), 4) AS sqft_avg_3_share,
        PERCENT_RANK() OVER (PARTITION BY region ORDER BY next_sqft_avg_3 DESC NULLS LAST) AS next_sqft_avg_3_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft_avg_6_prior_share, id) AS seq_no
    FROM metrics_stores
),
daily AS (
    SELECT
        CASE WHEN sqft_avg_3 >= 13637 THEN 'HIGH' ELSE 'LOW' END AS sqft_avg_3_tier,
        CASE WHEN prev_sqft_avg_3 >= 17766 THEN 'LARGE' ELSE 'SMALL' END AS prev_sqft_avg_3_tier,
        region,
        SIZE(COLLECT_SET(region)) AS num_regions,
        APPROX_PERCENTILE(next_city_sum_sqft, ARRAY(0.25, 0.5, 0.75)) AS next_city_sum_sqft_quartiles,
        COUNT_IF(region = 'Central') AS central_events
    FROM enriched
    GROUP BY 1, 2, 3
)
SELECT
    sqft_avg_3_tier,
    prev_sqft_avg_3_tier,
    region,
    central_events
FROM daily;
