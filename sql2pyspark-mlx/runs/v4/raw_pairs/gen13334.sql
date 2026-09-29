WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(city) IGNORE NULLS OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city,
        LAG(age, 2) OVER (PARTITION BY country ORDER BY age, id) AS prev_age
    FROM customers
),
scored_customers AS (
    -- Running and moving aggregates
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
        age,
        last_known_city,
        prev_age,
        CORR(age, credit_limit) OVER (PARTITION BY country) AS corr_age_credit_limit,
        AVG(prev_age) OVER (PARTITION BY city) AS city_avg_prev_age,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date DESC, id) AS event_seq,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC) AS credit_limit_dense_rank
    FROM scored
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(event_seq / city_avg_prev_age, 2) AS event_seq_per_city_avg_prev_age,
        ROUND(event_seq / age, 2) AS event_seq_per_age
    FROM scored_customers
    WHERE signup_date >= DATE '2023-06-29'
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    CASE WHEN event_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_tier,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
    COUNT(DISTINCT segment) AS distinct_segments,
    SUM(credit_limit) AS total_credit_limit
FROM cleaned
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', signup_date), CASE WHEN event_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('QUARTER', signup_date)), ());
