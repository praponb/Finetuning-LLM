WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        credit_limit,
        CASE WHEN segment = 'Consumer' THEN 1 ELSE 0 END is_consumer,
        LEAST(credit_limit, age) least_credit_limit_age,
        COALESCE(city, 'NONE') city_filled
    FROM customers
    WHERE city IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        country,
        segment,
        least_credit_limit_age,
        city_filled,
        CUME_DIST() OVER (PARTITION BY city, country ORDER BY credit_limit DESC NULLS LAST) credit_limit_cume_dist,
        CUME_DIST() OVER (PARTITION BY city ORDER BY is_consumer NULLS FIRST) is_consumer_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY least_credit_limit_age DESC, id) event_seq,
        MAX(is_consumer) OVER (PARTITION BY country, city) country_max_is_consumer
    FROM base
    WHERE credit_limit BETWEEN 6900 AND 9100
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(is_consumer_cume_dist) OVER (PARTITION BY segment, country ORDER BY credit_limit_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_avg_is_consumer_cume_dist,
        SUM(is_consumer_cume_dist) OVER (PARTITION BY segment, country ORDER BY credit_limit_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_sum_is_consumer_cume_dist,
        MAX(is_consumer_cume_dist) OVER (PARTITION BY segment, country ORDER BY credit_limit_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_max_is_consumer_cume_dist
    FROM enriched
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.shipping_fee >= 39
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.customer_id = 40
        )
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        segment,
        country,
        least_credit_limit_age
    FROM not_flagged
) src
PIVOT (
    SUM(least_credit_limit_age) AS value_sum,
    COUNT(1) AS n
    FOR country IN ('JP', 'SG', 'UK')
);
