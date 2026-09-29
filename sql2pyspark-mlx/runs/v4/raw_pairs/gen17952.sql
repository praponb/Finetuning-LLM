WITH base AS (
    SELECT
        *,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(amount, 0) AS amount_filled
    FROM transactions
    WHERE txn_time < '2023-05-01'
),
joined AS (
    SELECT src.*, d.account_type
    FROM base src
    JOIN accounts d
        ON src.account_id = d.id
),
enriched AS (
    SELECT
        src.*,
        p.balance,
        p.status,
        p.account_type AS account_account_type
    FROM joined src
    LEFT JOIN accounts p
        ON src.account_id = p.id
),
base_transactions AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        is_flagged_flag,
        account_type,
        balance,
        account_account_type,
        SUBSTRING(status, 1, 4) AS status_prefix,
        COALESCE(account_account_type, 'NONE') AS resolved_account_account_type
    FROM enriched
    WHERE txn_time > DATE '2023-12-12'
),
joined_transactions AS (
    SELECT
        s.*,
        a.opened_date,
        a.account_type AS account_account_type_2
    FROM base_transactions s
    LEFT JOIN accounts a
        ON s.account_id = a.id
)
SELECT
    id,
    account_id,
    amount,
    txn_time,
    merchant,
    is_flagged_flag,
    account_type,
    account_account_type,
    status_prefix,
    CASE WHEN account_type RLIKE '^(SAVINGS|CHECKING)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS account_type_zone
FROM joined_transactions;
