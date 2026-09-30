WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        TRIM(city) clean_city,
        COALESCE(segment, 'OTHER') segment_filled
    FROM customers
    WHERE signup_date > DATE '2023-12-08'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.browser = 'Safari'
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY segment ORDER BY credit_limit DESC) credit_limit_percent_rank,
        FIRST_VALUE(age) IGNORE NULLS OVER (PARTITION BY city, segment ORDER BY credit_limit ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_age,
        NTILE(4) OVER (ORDER BY credit_limit ASC, id ASC) credit_limit_bucket
    FROM eligible
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(age AS DOUBLE) age_str,
        GREATEST(credit_limit, first_known_age) greatest_credit_limit_first_known_age,
        COALESCE(segment, 'UNKNOWN') resolved_segment
    FROM scored
    WHERE first_known_age <= 36
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY credit_limit_percent_rank DESC) AS latest_rank
        FROM valid
    ) x
    WHERE latest_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        clean_city,
        segment_filled,
        credit_limit_percent_rank,
        first_known_age,
        credit_limit_bucket,
        age_str,
        greatest_credit_limit_first_known_age,
        resolved_segment,
        latest_rank,
        CUME_DIST() OVER (ORDER BY first_known_age DESC) first_known_age_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit ASC, id ASC) event_seq,
        NTILE(10) OVER (ORDER BY credit_limit_bucket DESC, id ASC) credit_limit_bucket_bucket,
        PERCENT_RANK() OVER (PARTITION BY country ORDER BY credit_limit_bucket DESC) credit_limit_bucket_percent_rank,
        AVG(age) OVER (ORDER BY credit_limit_bucket ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) age_avg_6_prior
    FROM latest
)
SELECT
    id,
    name,
    country,
    age,
    clean_city,
    segment_filled,
    first_known_age,
    credit_limit_bucket,
    greatest_credit_limit_first_known_age,
    CASE WHEN country RLIKE '^(DE|UK)$' THEN 'CORE' ELSE 'GROUP_B' END AS country_zone
FROM enriched
ORDER BY age DESC;
