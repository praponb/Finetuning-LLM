WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        is_active,
        age,
        LOWER(segment) AS segment_lower,
        DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date,
        COALESCE(city, 'UNKNOWN') AS resolved_city
    FROM customers
    WHERE signup_date > DATE '2023-09-30'
      AND segment IS NOT NULL
      AND NOT is_active
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date NULLS LAST, id) AS dedup_rank
        FROM base
    ) t
    WHERE dedup_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        is_active,
        age,
        segment_lower,
        days_since_signup_date,
        resolved_city,
        dedup_rank,
        SUM(age) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_age,
        ROUND(age / SUM(age) OVER (PARTITION BY city), 4) AS age_share,
        CUME_DIST() OVER (ORDER BY age DESC NULLS FIRST) AS age_cume_dist
    FROM latest
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        is_active,
        age,
        segment_lower,
        days_since_signup_date,
        resolved_city,
        dedup_rank,
        cumulative_age,
        age_share,
        age_cume_dist,
        AVG(age_cume_dist) OVER (PARTITION BY segment ORDER BY cumulative_age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS age_cume_dist_avg_3,
        RANK() OVER (PARTITION BY country ORDER BY days_since_signup_date NULLS FIRST) AS days_since_signup_date_rank
    FROM enriched
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        city,
        age_share,
        age_cume_dist
    FROM scored
) src
UNPIVOT (
    reading FOR measure IN (age_share AS AGE_SHARE, age_cume_dist AS AGE_CUME_DIST_VAL)
);
