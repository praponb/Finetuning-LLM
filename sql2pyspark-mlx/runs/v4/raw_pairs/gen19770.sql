WITH matched_keys AS (
    SELECT account_type, status
    FROM accounts
    EXCEPT
    SELECT account_type, status
    FROM accounts
    WHERE customer_id IS NULL
),
verified AS (
    SELECT s.*
    FROM accounts s
    INNER JOIN matched_keys k ON s.account_type = k.account_type AND s.status = k.status
),
enriched AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        RANK() OVER (PARTITION BY account_type ORDER BY balance DESC) AS balance_rank,
        RANK() OVER (PARTITION BY account_type ORDER BY balance DESC NULLS LAST) AS balance_rank_2
    FROM verified
),
metrics AS (
    SELECT
        *,
        ROUND(balance_rank_2 / SUM(balance_rank_2) OVER (PARTITION BY account_type), 4) AS balance_rank_2_share,
        ROUND(balance_rank / SUM(balance_rank) OVER (PARTITION BY account_type, status), 4) AS balance_rank_share,
        PERCENT_RANK() OVER (PARTITION BY account_type ORDER BY balance_rank_2 DESC NULLS LAST) AS balance_rank_2_percent_rank
    FROM enriched
),
kept AS (
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
        )
),
with_dims AS (
    SELECT
        src.*,
        d.segment,
        d.country
    FROM kept src
    LEFT JOIN customers d
        ON src.customer_id = d.id
)
SELECT
    account_type,
    balance,
    opened_date,
    status,
    balance_rank,
    balance_rank_2,
    balance_rank_share,
    balance_rank_2_percent_rank,
    segment
FROM with_dims
ORDER BY balance_rank_2 DESC NULLS FIRST;
