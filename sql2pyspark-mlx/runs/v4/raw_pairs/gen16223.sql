WITH filtered AS (
    SELECT
        *,
        CONCAT(segment, '/', country) AS segment_country,
        INITCAP(TRIM(city)) AS city_title
    FROM customers
    WHERE signup_date > DATE '2023-08-03'
      AND credit_limit < 11300
),
deduped AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit DESC, id) = 1
),
kept_keys AS (
    SELECT id
    FROM deduped
    EXCEPT
    SELECT id
    FROM customers
    WHERE credit_limit BETWEEN 7100 AND 12100
),
verified AS (
    SELECT s.*
    FROM deduped s
    INNER JOIN kept_keys k ON s.id = k.id
),
daily AS (
    SELECT
        DATE_TRUNC('WEEK', signup_date) AS week_start,
        CASE WHEN credit_limit >= 12600 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
        city,
        SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
        ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
        COUNT(*) AS event_count,
        ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN segment = 'Consumer' THEN 1 END), 0), 2) AS credit_limit_per_consumer,
        SUM(CASE WHEN city = 'Bangkok' THEN age ELSE 0 END) AS bangkok_age,
        BIT_OR(age) AS age_bit_or
    FROM verified
    GROUP BY DATE_TRUNC('WEEK', signup_date), CASE WHEN credit_limit >= 12600 THEN 'PREMIUM' ELSE 'STANDARD' END, city
    HAVING ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN segment = 'Consumer' THEN 1 END), 0), 2) < 0
)
SELECT
    week_start,
    credit_limit_tier,
    city,
    segment_list,
    avg_credit_limit,
    event_count,
    credit_limit_per_consumer,
    bangkok_age,
    age_bit_or
FROM daily
