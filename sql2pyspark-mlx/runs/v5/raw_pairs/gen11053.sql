WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(credit_limit) OVER w_grp AS prev_credit_limit,
        LEAD(age) OVER w_grp AS next_age,
        ROW_NUMBER() OVER w_grp AS event_seq,
        STDDEV_SAMP(credit_limit) OVER (PARTITION BY city) AS city_stddev_samp_credit_limit
    FROM customers
    WINDOW w_grp AS (PARTITION BY country ORDER BY age, id)
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.opened_date < '2023-04-01'
        )
),
with_last AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.status
    FROM eligible s
    JOIN LATERAL (
            SELECT c.status
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        segment,
        signup_date,
        is_active,
        credit_limit,
        prev_credit_limit,
        event_seq,
        city_stddev_samp_credit_limit,
        SUBSTRING(status, 1, 4) AS status_prefix
    FROM with_last
    WHERE segment IS NOT DISTINCT FROM 'SMB'
      AND is_active = FALSE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        segment,
        signup_date,
        credit_limit,
        prev_credit_limit,
        event_seq,
        city_stddev_samp_credit_limit,
        status_prefix,
        RANK() OVER w_running AS city_stddev_samp_credit_limit_rank,
        MAX(prev_credit_limit) OVER (PARTITION BY segment) AS segment_max_prev_credit_limit,
        SUM(credit_limit) OVER (ORDER BY city_stddev_samp_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY segment), 4) AS credit_limit_share
    FROM filtered
    WHERE city_stddev_samp_credit_limit IS NULL
    WINDOW w_running AS (PARTITION BY segment ORDER BY city_stddev_samp_credit_limit DESC NULLS LAST)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(segment) AS segment_upper,
        LOWER(name) AS name_lower
    FROM metrics
    WHERE segment IS NOT DISTINCT FROM 'Consumer'
)
-- Multi-level aggregation
SELECT
    segment,
    CASE WHEN event_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_tier,
    PERCENTILE_APPROX(prev_credit_limit, ARRAY(0.25, 0.5, 0.75)) AS prev_credit_limit_quartiles,
    ROUND(SUM(running_credit_limit) / NULLIF(COUNT(CASE WHEN segment = 'Corporate' THEN 1 END), 0), 2) AS running_credit_limit_per_corporate,
    COUNT(*) AS row_count,
    SUM(CASE WHEN segment = 'Corporate' THEN running_credit_limit ELSE 0 END) AS corporate_running_credit_limit,
    COUNT(*) AS row_count_2
FROM valid
GROUP BY 1, 2
ORDER BY row_count DESC NULLS LAST
