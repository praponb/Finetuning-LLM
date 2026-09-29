WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(country) AS country_length,
        DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at,
        COALESCE(country, 'OTHER') AS resolved_country
    FROM sessions
    WHERE started_at > DATE '2023-08-28'
      AND country IS NOT DISTINCT FROM 'TH'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        country,
        days_since_started_at,
        resolved_country,
        SUM(days_since_started_at) OVER (ORDER BY days_since_started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_days_since_started_at,
        NTILE(5) OVER (PARTITION BY country, browser ORDER BY country_length DESC, id ASC) AS country_length_bucket
    FROM valid
    WHERE days_since_started_at BETWEEN 39 AND 73
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        days_since_started_at,
        CUME_DIST() OVER (PARTITION BY customer_id, country ORDER BY cumulative_days_since_started_at DESC NULLS FIRST) AS cumulative_days_since_started_at_cume_dist,
        LAST_VALUE(country_length_bucket, TRUE) OVER (PARTITION BY customer_id, country ORDER BY country_length_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country_length_bucket
    FROM enriched
),
enriched_sessions AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_known_country_length_bucket ASC, id ASC) AS seq_no,
        AVG(last_known_country_length_bucket) OVER (PARTITION BY customer_id ORDER BY days_since_started_at ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS last_known_country_length_bucket_avg_3_prior
    FROM with_windows
),
enriched_sessions_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(seq_no) OVER (PARTITION BY customer_id ORDER BY last_known_country_length_bucket_avg_3_prior ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_known_country_length_bucket_avg_3_prior DESC, id ASC) AS event_seq,
        LAG(last_known_country_length_bucket, 1) OVER (PARTITION BY customer_id ORDER BY cumulative_days_since_started_at_cume_dist ASC, id ASC) AS prev_last_known_country_length_bucket,
        LAG(last_known_country_length_bucket, 1) OVER (PARTITION BY customer_id ORDER BY last_known_country_length_bucket_avg_3_prior ASC, id ASC) AS prev_last_known_country_length_bucket_2
    FROM enriched_sessions
)
-- Multi-level aggregation
SELECT
    CASE WHEN cumulative_days_since_started_at_cume_dist >= 0.86 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_days_since_started_at_cume_dist_tier,
    seq_no,
    APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
    PERCENTILE_APPROX(cumulative_days_since_started_at_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS cumulative_days_since_started_at_cume_dist_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(prev_last_known_country_length_bucket_2, ARRAY(0.25, 0.5, 0.75)) AS prev_last_known_country_length_bucket_2_quartiles,
    GROUPING(CASE WHEN cumulative_days_since_started_at_cume_dist >= 0.86 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
FROM enriched_sessions_2
GROUP BY GROUPING SETS ((CASE WHEN cumulative_days_since_started_at_cume_dist >= 0.86 THEN 'PREMIUM' ELSE 'STANDARD' END, seq_no), (CASE WHEN cumulative_days_since_started_at_cume_dist >= 0.86 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
