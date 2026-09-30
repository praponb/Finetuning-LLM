WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        DATE_SUB(opened_date, 54) AS opened_date_minus_54d,
        COALESCE(status, 'N/A') AS status_clean
    FROM accounts
    WHERE customer_id IS NOT NULL
      AND customer_id IS NULL
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.transaction_txn_time
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.segment,
        p.name,
        p.signup_date
    FROM latest_match src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            account_type,
            customer_id,
            segment,
            balance
        FROM with_dims
    ) src
    PIVOT (
        AVG(balance) AS amount,
        COUNT(1) AS cnt
        FOR segment IN ('Corporate' AS corporate, 'SMB' AS smb)
    )
)
SELECT
    account_type,
    customer_id,
    corporate_amount,
    corporate_cnt,
    smb_amount,
    smb_cnt
FROM wide
ORDER BY smb_amount;
