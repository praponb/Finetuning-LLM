WITH cleaned AS (
    SELECT
        *,
        LENGTH(account_type) AS account_type_length,
        CONCAT_WS('-', status, account_type) AS status_account_type_key
    FROM accounts
    WHERE opened_date > '2025-10-01'
),
matched_keys AS (
    SELECT id, customer_id
    FROM cleaned
    INTERSECT
    SELECT id, customer_id
    FROM accounts
    WHERE account_type = 'LOAN'
),
kept_rows AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
kept AS (
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_type IN ('DEBIT', 'CREDIT')
        )
),
eligible AS (
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time >= s.opened_date
        )
)
SELECT
    *,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY account_type_length DESC) AS account_type_length_dense_rank,
    MIN(account_type_length) OVER (PARTITION BY customer_id, status) AS customer_id_min_account_type_length,
    MAX(account_type_length) OVER (PARTITION BY account_type) AS account_type_max_account_type_length,
    CUME_DIST() OVER (PARTITION BY customer_id ORDER BY account_type_length) AS account_type_length_cume_dist,
    RANK() OVER (PARTITION BY customer_id, account_type ORDER BY account_type_length DESC) AS account_type_length_rank
FROM eligible;
