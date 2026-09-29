WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS balance_avg_6,
        FIRST_VALUE(status) IGNORE NULLS OVER w_hist AS first_known_status
    FROM accounts
    WINDOW w_hist AS (PARTITION BY account_type, status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance_avg_6 DESC NULLS LAST, id) AS rn
        FROM enriched
    ) t
    WHERE rn = 1
),
ranked_accounts AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS rn_2
        FROM ranked
    ) t
    WHERE rn_2 = 1
)
SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    status,
    rn,
    rn_2
FROM ranked_accounts
ORDER BY customer_id
