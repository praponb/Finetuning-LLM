WITH valid AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        LOWER(city) AS city_lower,
        COALESCE(city, 'NONE') AS city_filled
    FROM customers
    WHERE city IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        name,
        city,
        segment,
        city_lower,
        ROW_NUMBER() OVER (ORDER BY credit_limit ASC, id ASC) AS row_seq,
        AVG(credit_limit) OVER (PARTITION BY country, city ORDER BY credit_limit ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS credit_limit_avg_3_prior,
        AVG(credit_limit) OVER (PARTITION BY segment) AS segment_avg_credit_limit
    FROM valid
),
cleaned AS (
    SELECT
        id,
        name,
        row_seq,
        credit_limit_avg_3_prior,
        segment_avg_credit_limit,
        CAST(segment_avg_credit_limit AS STRING) AS segment_avg_credit_limit_num
    FROM with_windows
    WHERE credit_limit_avg_3_prior IS NOT NULL
),
kept AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.customer_id IN (10, 14, 15, 32)
        )
)
SELECT
    CASE WHEN credit_limit_avg_3_prior >= 15961 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_avg_3_prior_tier,
    APPROX_PERCENTILE(credit_limit_avg_3_prior, 0.9) AS credit_limit_avg_3_prior_p90,
    MAX(segment_avg_credit_limit) AS max_segment_avg_credit_limit,
    ROUND(AVG(credit_limit_avg_3_prior), 2) AS avg_credit_limit_avg_3_prior,
    APPROX_PERCENTILE(credit_limit_avg_3_prior, 0.9) AS credit_limit_avg_3_prior_p90_2,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    COUNT(*) AS row_count
FROM kept
GROUP BY CASE WHEN credit_limit_avg_3_prior >= 15961 THEN 'HIGH' ELSE 'LOW' END
ORDER BY max_segment_avg_credit_limit DESC NULLS FIRST;
