WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        segment,
        signup_date,
        credit_limit,
        age,
        MAX(credit_limit) OVER (PARTITION BY country, city) country_max_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id) row_seq
    FROM customers
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.start_date >= s.signup_date
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(row_seq) OVER w_recent last7_sum_row_seq,
        MAX(row_seq) OVER w_recent last7_max_row_seq,
        MIN(row_seq) OVER w_recent last7_min_row_seq,
        FIRST_VALUE(age, TRUE) OVER (PARTITION BY segment ORDER BY country_max_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_age
    FROM eligible
    WHERE segment IN ('Consumer', 'Corporate')
    WINDOW w_recent AS (PARTITION BY segment ORDER BY age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(country_max_credit_limit) OVER (PARTITION BY segment ORDER BY last7_max_row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_country_max_credit_limit,
        MAX(country_max_credit_limit) OVER (PARTITION BY segment ORDER BY last7_max_row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_country_max_credit_limit,
        AVG(country_max_credit_limit) OVER (PARTITION BY segment ORDER BY last7_max_row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_country_max_credit_limit,
        CORR(credit_limit, age) OVER (PARTITION BY segment) corr_credit_limit_age
    FROM scored
)
-- Aggregate with conditional metrics
SELECT
    segment,
    CASE WHEN row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    CASE WHEN last7_min_row_seq >= 6.4 THEN 'PREMIUM' ELSE 'STANDARD' END AS last7_min_row_seq_tier,
    COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) AS corporate_count,
    PERCENTILE_APPROX(running_max_country_max_credit_limit, ARRAY(0.25, 0.5, 0.75)) AS running_max_country_max_credit_limit_quartiles,
    COUNT(DISTINCT segment) AS distinct_segments,
    GROUPING_ID() AS grouping_level
FROM with_windows
GROUP BY GROUPING SETS ((segment, CASE WHEN row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN last7_min_row_seq >= 6.4 THEN 'PREMIUM' ELSE 'STANDARD' END), (segment, CASE WHEN row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END), (segment));
