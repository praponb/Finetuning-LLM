WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(is_active AS INT) is_active_flag
    FROM customers
    WHERE signup_date >= DATE '2023-05-19'
      AND segment IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date < s.signup_date
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        country,
        city,
        segment,
        credit_limit,
        is_active_flag,
        LAST_VALUE(credit_limit, TRUE) OVER (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_credit_limit,
        SUM(credit_limit) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_credit_limit
    FROM eligible
    WHERE credit_limit IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id) AS rank_in_group
    FROM with_windows
    QUALIFY rank_in_group = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.balance IS NOT NULL
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        name,
        country,
        credit_limit,
        is_active_flag,
        cumulative_credit_limit,
        NTILE(5) OVER (ORDER BY last_known_credit_limit, id) last_known_credit_limit_bucket,
        MAX(last_known_credit_limit) OVER (PARTITION BY city) city_max_last_known_credit_limit,
        SUM(cumulative_credit_limit) OVER (PARTITION BY country ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_cumulative_credit_limit
    FROM not_flagged
)
SELECT
    id,
    credit_limit,
    cumulative_credit_limit,
    last_known_credit_limit_bucket,
    city_max_last_known_credit_limit
FROM scored
