WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < s.opened_date
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY account_type ORDER BY balance DESC NULLS FIRST) AS balance_percent_rank,
        MAX(balance) OVER (PARTITION BY customer_id) AS customer_id_max_balance,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY opened_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS balance_avg_6,
        AVG(balance) OVER (PARTITION BY status ORDER BY balance ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS balance_avg_6_prior,
        FIRST_VALUE(balance, TRUE) OVER (PARTITION BY account_type ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance
    FROM not_flagged
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        customer_id_max_balance,
        balance_avg_6,
        balance_avg_6_prior,
        first_known_balance,
        MAX(first_known_balance) OVER (PARTITION BY status) AS status_max_first_known_balance,
        CUME_DIST() OVER (PARTITION BY customer_id, status ORDER BY balance_avg_6_prior DESC NULLS FIRST) AS balance_avg_6_prior_cume_dist,
        LAG(customer_id_max_balance, 1, 0) OVER w_running AS prev_customer_id_max_balance,
        PERCENT_RANK() OVER w_hist AS balance_percent_rank_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY account_type, status ORDER BY balance_percent_rank DESC, id ASC) AS seq_no
    FROM scored
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY opened_date ASC, id ASC), w_hist AS (PARTITION BY status ORDER BY balance_percent_rank ASC)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time < s.opened_date
        )
),
kept_accounts AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_type = 'DEBIT'
        )
)
-- Join a dimension and aggregate
SELECT
    dim.city,
    s.status,
    COUNT(*) AS num_rows,
    AVG(s.balance_avg_6_prior_cume_dist) AS avg_balance_avg_6_prior_cume_dist
FROM kept_accounts s
LEFT JOIN customers dim ON s.customer_id = dim.id
GROUP BY dim.city, s.status
ORDER BY num_rows ASC NULLS FIRST;
