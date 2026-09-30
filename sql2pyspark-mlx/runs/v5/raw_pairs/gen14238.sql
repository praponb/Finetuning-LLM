WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date,
        COALESCE(status, 'UNKNOWN') AS status_clean
    FROM accounts
    WHERE opened_date > DATE '2023-04-18'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC NULLS LAST, id) AS latest_rank
        FROM valid
    ) ranked
    WHERE latest_rank = 1
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.transaction_merchant,
        top1.transaction_amount
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(days_since_opened_date) OVER (PARTITION BY customer_id, account_type) AS customer_id_stddev_samp_days_since_opened_date,
        LAG(transaction_amount, 1) OVER (PARTITION BY customer_id, status ORDER BY days_since_opened_date, id) AS prev_transaction_amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY days_since_opened_date, id) AS seq_no
    FROM with_latest
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        days_since_opened_date,
        status_clean,
        latest_rank,
        transaction_merchant,
        customer_id_stddev_samp_days_since_opened_date,
        prev_transaction_amount,
        seq_no,
        LOWER(account_type) AS account_type_lower,
        COALESCE(transaction_merchant, 'NONE') AS transaction_merchant_clean
    FROM scored
    WHERE opened_date >= DATE '2023-03-30'
      AND customer_id IS NOT NULL
),
with_latest_accounts AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.balance,
        s.latest_rank,
        s.transaction_merchant,
        s.customer_id_stddev_samp_days_since_opened_date,
        s.prev_transaction_amount,
        s.seq_no,
        s.account_type_lower,
        s.transaction_merchant_clean,
        top1.txn_time
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.country,
        c.city,
        c.segment
    FROM with_latest_accounts a
    LEFT JOIN customers c
        ON a.customer_id = c.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    city,
    measure,
    value
FROM (
    SELECT
        id,
        city,
        balance,
        prev_transaction_amount
    FROM enriched
) src
UNPIVOT (
    value FOR measure IN (balance AS BALANCE, prev_transaction_amount)
);
