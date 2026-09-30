WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft NULLS LAST, id) AS rank_in_group
    FROM stores
    QUALIFY rank_in_group = 1
),
metrics AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        rank_in_group,
        AVG(sqft) OVER (ORDER BY sqft, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_avg_5_prior,
        MIN(sqft) OVER (PARTITION BY city) AS city_min_sqft
    FROM ranked
),
enriched AS (
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY city, region ORDER BY sqft_avg_5_prior, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS sqft_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY rank_in_group, id) AS seq_no
    FROM metrics
),
metrics_stores AS (
    SELECT
        *,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY city ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
        AVG(sqft) OVER (PARTITION BY city ORDER BY sqft_avg_5_prior, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sqft_avg_6,
        ROW_NUMBER() OVER (ORDER BY city_min_sqft, id) AS event_seq,
        SUM(sqft_avg_5_prior) OVER (ORDER BY sqft_avg_5_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft_avg_5_prior,
        ROUND(sqft_avg_3_prior / SUM(sqft_avg_3_prior) OVER (PARTITION BY city, region), 4) AS sqft_avg_3_prior_share
    FROM enriched
),
grouped AS (
    SELECT
        city,
        DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
        ROUND(AVG(seq_no), 2) AS avg_seq_no,
        SUM(CASE WHEN region = 'North' THEN sqft_avg_5_prior ELSE 0 END) AS north_sqft_avg_5_prior,
        AVG(sqft_avg_5_prior) AS avg_sqft_avg_5_prior
    FROM metrics_stores
    GROUP BY city, DATE_TRUNC('QUARTER', opened_date)
)
SELECT
    city,
    quarter_start,
    avg_seq_no
FROM grouped
ORDER BY city DESC
