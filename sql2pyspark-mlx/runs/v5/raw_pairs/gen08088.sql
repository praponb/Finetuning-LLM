WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(amount) OVER (PARTITION BY merchant, account_id ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_amount,
        COUNT(amount) OVER (PARTITION BY merchant, account_id ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_amount,
        MIN(amount) OVER (PARTITION BY merchant, account_id ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_amount
    FROM transactions
    WHERE merchant IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        amount,
        is_flagged,
        last4_max_amount,
        last4_count_amount,
        last4_min_amount,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY last4_count_amount DESC, id) AS row_seq,
        SUM(amount) OVER (ORDER BY last4_min_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        SUM(last4_count_amount) OVER (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last4_count_amount,
        SUM(last4_max_amount) OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last4_max_amount,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY last4_max_amount DESC, id) AS seq_no
    FROM enriched
),
kept_keys AS (
    -- Set difference on keys
    SELECT account_id, id
    FROM scored
    EXCEPT DISTINCT
    SELECT account_id, id
    FROM transactions
    WHERE NOT is_flagged
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.account_id = k.account_id AND s.id = k.id
),
joined AS (
    -- Attach accounts attributes
    SELECT e.*, b.opened_date
    FROM verified e
    LEFT JOIN accounts b
        ON e.account_id = b.id
),
joined_transactions AS (
    -- Attach accounts attributes
    SELECT
        src.account_id,
        src.last4_count_amount,
        src.row_seq,
        src.cumulative_last4_count_amount,
        src.cumulative_last4_max_amount,
        src.seq_no,
        src.opened_date,
        b.opened_date AS account_opened_date,
        b.account_type,
        b.balance
    FROM joined src
    LEFT JOIN accounts b
        ON src.account_id = b.id
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        p.balance AS account_balance,
        p.account_type AS account_account_type,
        p.status
    FROM joined_transactions e
    LEFT JOIN accounts p
        ON e.account_id = p.id
)
SELECT
    last4_count_amount,
    row_seq,
    cumulative_last4_max_amount,
    seq_no,
    account_opened_date,
    account_type,
    balance,
    account_account_type,
    status,
    CASE WHEN account_account_type RLIKE '^(LOAN|SAVINGS)$' THEN 'GROUP_A' WHEN account_account_type RLIKE '^(CHECKING|CREDIT)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS account_account_type_zone
FROM with_dims
ORDER BY cumulative_last4_max_amount DESC NULLS LAST;
