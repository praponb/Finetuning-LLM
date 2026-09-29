WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(age) OVER w_running next_age,
        LAG(credit_limit) OVER w_running prev_credit_limit,
        ROW_NUMBER() OVER w_running event_seq
    FROM customers
    WINDOW w_running AS (PARTITION BY country ORDER BY credit_limit, id)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date <= s.signup_date
        )
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM kept
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.device = 'mobile'
        )
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        prev_credit_limit,
        CAST(is_active AS INT) is_active_flag
    FROM not_flagged
    WHERE segment IS NOT NULL
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        city,
        MAX(is_active_flag) FILTER (WHERE segment = 'Consumer') AS consumer_max_is_active_flag,
        COUNT_IF(city = 'Chiang Mai') AS chiang_mai_events,
        COUNT(*) AS cnt,
        STDDEV_SAMP(age) AS age_stddev,
        ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN segment = 'SMB' THEN 1 END), 0), 2) AS credit_limit_per_smb
    FROM base
    GROUP BY city
    HAVING age_stddev >= 0
)
SELECT
    city,
    consumer_max_is_active_flag,
    chiang_mai_events,
    age_stddev,
    credit_limit_per_smb,
    CASE WHEN city RLIKE '^(New York|Singapore)$' THEN 'GROUP_B' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS city_zone
FROM totals
ORDER BY consumer_max_is_active_flag NULLS LAST
