WITH scored AS (
    -- Window metrics over several different partitions
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
        CORR(age, credit_limit) OVER w_grp corr_age_credit_limit,
        CUME_DIST() OVER (PARTITION BY city ORDER BY age NULLS LAST) age_cume_dist,
        LAST_VALUE(segment, TRUE) OVER w_hist last_known_segment,
        LAG(credit_limit, 2) OVER w_hist_2 prev_credit_limit,
        AVG(age) OVER (PARTITION BY city ORDER BY credit_limit, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) age_avg_6_prior
    FROM customers
    WINDOW w_grp AS (PARTITION BY segment, city), w_hist AS (PARTITION BY country, city ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist_2 AS (PARTITION BY country ORDER BY signup_date, id)
),
latest_match AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.account_opened_date
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.opened_date AS account_opened_date
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date <= s.signup_date
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT country, segment
    FROM not_flagged
    EXCEPT
    SELECT country, segment
    FROM customers
    WHERE signup_date > '2024-05-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    INNER JOIN remaining_keys k ON s.country = k.country AND s.segment = k.segment
)
-- Multi-level aggregation
SELECT
    country,
    COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) AS corporate_count,
    COUNT_IF(segment = 'Consumer') AS consumer_events,
    COUNT(DISTINCT country) AS distinct_countries,
    ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN city = 'Bangkok' THEN 1 END), 0), 2) AS age_per_bangkok
FROM verified
GROUP BY country
HAVING corporate_count >= 1
ORDER BY distinct_countries NULLS LAST
