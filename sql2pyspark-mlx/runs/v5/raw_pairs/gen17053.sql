WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY status ORDER BY balance DESC NULLS LAST) AS balance_rank,
        NTILE(5) OVER (PARTITION BY account_type ORDER BY balance DESC, id) AS balance_bucket,
        LAG(balance, 2) OVER (PARTITION BY customer_id ORDER BY balance, id) AS prev_balance,
        LAST_VALUE(status, TRUE) OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS balance_avg_5_prior
    FROM accounts
    WHERE account_type IN ('LOAN', 'SAVINGS')
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.merchant = 'Shell'
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.account_id IN (31, 39)
        )
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.account_type,
        src.balance,
        src.opened_date,
        src.balance_rank,
        src.prev_balance,
        src.last_known_status,
        src.balance_avg_5_prior,
        c.segment,
        c.signup_date
    FROM kept src
    INNER JOIN customers c
        ON src.customer_id = c.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM joined
    EXCEPT
    SELECT id
    FROM accounts
    WHERE balance > 59000
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN matched_keys k ON s.id = k.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.credit_limit,
        p.country
    FROM reconciled s
    INNER JOIN customers p
        ON s.customer_id = p.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY credit_limit DESC NULLS LAST) AS credit_limit_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY account_type ORDER BY balance DESC) AS balance_percent_rank
    FROM enriched
)
SELECT
    id,
    customer_id,
    balance,
    balance_rank,
    balance_avg_5_prior,
    segment,
    country,
    balance_percent_rank
FROM metrics
ORDER BY id NULLS LAST;
