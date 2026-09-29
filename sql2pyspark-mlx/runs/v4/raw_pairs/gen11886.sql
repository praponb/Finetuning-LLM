WITH filtered AS (
    SELECT
        *,
        CAST(age AS DOUBLE) AS age_num,
        COALESCE(city, 'N/A') AS city_filled
    FROM customers
    WHERE signup_date >= TO_DATE('2023-08-17')
      AND segment = 'Corporate'
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER w_part AS age_num_dense_rank,
        LEAD(age_num, 1) OVER w_part_2 AS next_age_num,
        ROUND(age / SUM(age) OVER (PARTITION BY segment), 4) AS age_share,
        SUM(age_num) OVER (PARTITION BY segment, country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age_num
    FROM filtered
    WINDOW w_part AS (PARTITION BY city ORDER BY age_num), w_part_2 AS (PARTITION BY segment ORDER BY signup_date, id)
),
scored_customers AS (
    SELECT
        *,
        STDDEV_SAMP(age_num_dense_rank) OVER (PARTITION BY country) AS country_stddev_samp_age_num_dense_rank,
        NTILE(5) OVER w_recent AS age_num_dense_rank_bucket,
        FIRST_VALUE(segment) IGNORE NULLS OVER w_running AS first_known_segment
    FROM scored
    WINDOW w_recent AS (ORDER BY age_num_dense_rank, id), w_running AS (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        *,
        LAG(age_num_dense_rank, 2, 0) OVER (PARTITION BY segment ORDER BY age_num_dense_rank, id) AS prev_age_num_dense_rank,
        ROUND(country_stddev_samp_age_num_dense_rank / SUM(country_stddev_samp_age_num_dense_rank) OVER (PARTITION BY segment), 4) AS country_stddev_samp_age_num_dense_rank_share,
        ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY credit_limit DESC, id) AS event_seq,
        FIRST_VALUE(age_num_dense_rank) IGNORE NULLS OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_age_num_dense_rank
    FROM scored_customers
)
SELECT
    CASE WHEN credit_limit >= 12500 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_tier,
    ROUND(SUM(age_num_dense_rank_bucket) / NULLIF(COUNT(CASE WHEN segment = 'SMB' THEN 1 END), 0), 2) AS age_num_dense_rank_bucket_per_smb,
    SUM(CASE WHEN country = 'TH' THEN age_num_dense_rank ELSE 0 END) AS th_age_num_dense_rank,
    COUNT(*) AS event_count
FROM enriched
GROUP BY credit_limit_tier
HAVING th_age_num_dense_rank > 0
ORDER BY credit_limit_tier;
