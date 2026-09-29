WITH valid AS (
    -- Filter the raw rows and derive helper columns
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
        GREATEST(credit_limit, age) AS greatest_credit_limit_age,
        TRIM(country) AS clean_country
    FROM customers
    WHERE signup_date > DATE '2024-04-15'
      AND city IS NOT NULL
),
latest_match AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.end_date,
        top1.subscription_start_date
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.end_date, c.start_date AS subscription_start_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Ranking and gap features
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
        greatest_credit_limit_age,
        clean_country,
        end_date,
        subscription_start_date,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY subscription_start_date, id) AS seq_no,
        AVG(credit_limit) OVER (PARTITION BY segment ORDER BY end_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS credit_limit_avg_6
    FROM latest_match
),
matched_keys AS (
    -- Set intersection on keys
    SELECT country
    FROM with_windows
    INTERSECT
    SELECT country
    FROM customers
    WHERE email IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.country = k.country
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at >= s.signup_date
        )
),
scored AS (
    -- Ranking and gap features
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
        greatest_credit_limit_age,
        clean_country,
        end_date,
        subscription_start_date,
        seq_no,
        credit_limit_avg_6,
        ROUND(credit_limit_avg_6 / SUM(credit_limit_avg_6) OVER (PARTITION BY city), 4) AS credit_limit_avg_6_share,
        CUME_DIST() OVER (PARTITION BY city ORDER BY credit_limit_avg_6 DESC NULLS FIRST) AS credit_limit_avg_6_cume_dist,
        RANK() OVER (PARTITION BY city ORDER BY greatest_credit_limit_age NULLS FIRST) AS greatest_credit_limit_age_rank
    FROM not_flagged
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', signup_date) AS week_start,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
    COUNT(*) FILTER (WHERE segment = 'Consumer') AS consumer_rows,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles,
    ROUND(SUM(credit_limit_avg_6_share) / NULLIF(COUNT(CASE WHEN country = 'TH' THEN 1 END), 0), 2) AS credit_limit_avg_6_share_per_th,
    APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles
FROM scored
GROUP BY DATE_TRUNC('WEEK', signup_date)
ORDER BY credit_limit_avg_6_share_per_th DESC NULLS FIRST;
