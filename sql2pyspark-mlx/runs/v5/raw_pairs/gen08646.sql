WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        credit_limit,
        CONCAT_WS('/', city, country) AS city_country_key
    FROM customers
    WHERE signup_date >= DATE '2023-06-12'
      AND city IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        city,
        segment,
        credit_limit,
        city_country_key,
        COALESCE(credit_limit, 0) AS credit_limit_filled,
        CASE WHEN credit_limit >= 12600 THEN 'High' WHEN credit_limit >= 2500 THEN 'Medium' ELSE 'Low' END AS credit_limit_level
    FROM valid
    WHERE city IN ('London', 'Tokyo')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(credit_limit_filled) IGNORE NULLS OVER w_ordered AS first_known_credit_limit_filled,
        SUM(credit_limit_filled) OVER w_recent AS running_credit_limit_filled,
        AVG(credit_limit) OVER (ORDER BY credit_limit, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS credit_limit_avg_3_prior
    FROM cleaned
    WHERE credit_limit_filled <= 28
    WINDOW w_ordered AS (PARTITION BY segment, city ORDER BY credit_limit_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        credit_limit,
        city_country_key,
        first_known_credit_limit_filled,
        running_credit_limit_filled,
        LEAD(credit_limit_avg_3_prior) OVER (PARTITION BY segment ORDER BY running_credit_limit_filled, id) AS next_credit_limit_avg_3_prior,
        LAG(running_credit_limit_filled) OVER (PARTITION BY segment ORDER BY running_credit_limit_filled, id) AS prev_running_credit_limit_filled,
        LEAD(running_credit_limit_filled) OVER (PARTITION BY segment ORDER BY running_credit_limit_filled, id) AS next_running_credit_limit_filled,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY running_credit_limit_filled, id) AS event_seq
    FROM metrics
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        first_known_credit_limit_filled,
        LEAD(first_known_credit_limit_filled, 1) OVER (PARTITION BY city ORDER BY event_seq, id) AS next_first_known_credit_limit_filled,
        RANK() OVER (ORDER BY running_credit_limit_filled DESC NULLS LAST) AS running_credit_limit_filled_rank,
        COUNT(first_known_credit_limit_filled) OVER (PARTITION BY city) AS city_count_first_known_credit_limit_filled
    FROM enriched
),
enriched_customers AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        first_known_credit_limit_filled,
        next_first_known_credit_limit_filled,
        AVG(first_known_credit_limit_filled) OVER w_hist AS running_avg_first_known_credit_limit_filled,
        MAX(first_known_credit_limit_filled) OVER w_hist AS running_max_first_known_credit_limit_filled,
        COUNT(first_known_credit_limit_filled) OVER w_hist AS running_count_first_known_credit_limit_filled,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY city_count_first_known_credit_limit_filled DESC, id) AS seq_no
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY city ORDER BY first_known_credit_limit_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        next_first_known_credit_limit_filled,
        running_avg_first_known_credit_limit_filled,
        running_max_first_known_credit_limit_filled,
        CASE WHEN first_known_credit_limit_filled >= 80 THEN 'High' WHEN first_known_credit_limit_filled >= 15 THEN 'Medium' ELSE 'Low' END AS first_known_credit_limit_filled_band,
        COALESCE(running_avg_first_known_credit_limit_filled, 0) AS running_avg_first_known_credit_limit_filled_filled
    FROM enriched_customers
    WHERE running_count_first_known_credit_limit_filled <= 6
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_max_first_known_credit_limit_filled >= 70 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_max_first_known_credit_limit_filled_tier,
    MIN(running_avg_first_known_credit_limit_filled_filled) AS min_running_avg_first_known_credit_limit_filled_filled,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(running_max_first_known_credit_limit_filled, 0.95) AS running_max_first_known_credit_limit_filled_p95,
    MIN(running_avg_first_known_credit_limit_filled) AS min_running_avg_first_known_credit_limit_filled
FROM base
GROUP BY CASE WHEN running_max_first_known_credit_limit_filled >= 70 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY running_max_first_known_credit_limit_filled_tier DESC NULLS FIRST
