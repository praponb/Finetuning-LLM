WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance, id) AS latest_rank
        FROM accounts
    ) t
    WHERE latest_rank = 1
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.signup_date,
        c.country,
        c.credit_limit
    FROM latest a
    INNER JOIN customers c
        ON a.customer_id = c.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time < s.opened_date
        )
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    SIZE(COLLECT_SET(country)) AS num_countries,
    PERCENTILE_APPROX(credit_limit, 0.5) AS credit_limit_p50,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list
FROM not_flagged
GROUP BY activity_month
