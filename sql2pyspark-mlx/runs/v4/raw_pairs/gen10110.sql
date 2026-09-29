WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY country, segment ORDER BY credit_limit DESC) AS credit_limit_dense_rank,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY credit_limit DESC NULLS FIRST) AS credit_limit_dense_rank_2,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC NULLS FIRST) AS credit_limit_percent_rank
    FROM customers
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, country
    FROM eligible
    EXCEPT
    SELECT id, country
    FROM customers
    WHERE signup_date BETWEEN '2023-09-19' AND '2023-12-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    JOIN matched_keys k ON s.id = k.id AND s.country = k.country
)
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
    age,
    credit_limit_dense_rank,
    credit_limit_dense_rank_2,
    credit_limit_percent_rank,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY credit_limit_percent_rank NULLS FIRST) AS credit_limit_percent_rank_dense_rank,
    ROUND(age / SUM(age) OVER (PARTITION BY segment), 4) AS age_share,
    ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY segment), 4) AS credit_limit_share,
    ROUND(age / SUM(age) OVER (PARTITION BY city), 4) AS age_share_2,
    RANK() OVER (PARTITION BY segment ORDER BY age DESC NULLS LAST) AS age_rank
FROM kept_rows
