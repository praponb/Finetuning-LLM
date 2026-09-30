WITH matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM accounts src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.opened_date >= sub.start_date
       AND src.opened_date < sub.end_date + INTERVAL '14' DAY
),
matched_keys AS (
    -- Set difference on keys
    SELECT account_type, id
    FROM matched
    EXCEPT
    SELECT account_type, id
    FROM accounts
    WHERE opened_date BETWEEN '2023-06-01' AND '2023-09-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM matched s
    INNER JOIN matched_keys k ON s.account_type = k.account_type AND s.id = k.id
),
enriched AS (
    -- Attach customers attributes
    SELECT e.*, d.credit_limit
    FROM reconciled e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        c.segment,
        c.country
    FROM enriched s
    INNER JOIN customers c
        ON s.customer_id = c.id
),
matched_keys_2 AS (
    -- Set intersection on keys
    SELECT id
    FROM joined
    INTERSECT ALL
    SELECT id
    FROM accounts
    WHERE status IN ('FROZEN', 'OPEN', 'CLOSED')
),
reconciled_accounts AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN matched_keys_2 k ON s.id = k.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY segment), 4) AS credit_limit_share,
        PERCENT_RANK() OVER (ORDER BY credit_limit) AS credit_limit_percent_rank,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY country), 4) AS monthly_fee_share,
        PERCENT_RANK() OVER w_running AS monthly_fee_percent_rank
    FROM reconciled_accounts
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY monthly_fee DESC NULLS LAST)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_type = 'REFUND'
        )
)
SELECT
    id,
    account_type,
    status,
    plan,
    monthly_fee,
    country,
    credit_limit_share,
    monthly_fee_share,
    monthly_fee_percent_rank
FROM kept;
