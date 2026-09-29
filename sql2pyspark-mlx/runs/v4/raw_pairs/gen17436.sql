WITH not_flagged AS (
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time >= s.opened_date
        )
),
enriched AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        LEAD(balance, 1) OVER (PARTITION BY status ORDER BY balance, id) AS next_balance,
        AVG(balance) OVER w_part AS balance_avg_5_prior,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM not_flagged
    WINDOW w_part AS (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_recent AS (PARTITION BY status ORDER BY opened_date DESC, id)
),
joined AS (
    SELECT
        s.*,
        d.country,
        d.email,
        d.city
    FROM enriched s
    JOIN customers d
        ON s.customer_id = d.id
),
with_windows AS (
    SELECT
        *,
        FIRST_VALUE(next_balance, TRUE) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_balance,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY seq_no DESC NULLS LAST) AS seq_no_dense_rank
    FROM joined
)
SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    status,
    next_balance,
    balance_avg_5_prior,
    seq_no
FROM with_windows
