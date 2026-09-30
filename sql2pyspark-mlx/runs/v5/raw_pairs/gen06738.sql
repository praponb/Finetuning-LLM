WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        UPPER(txn_type) txn_type_upper
    FROM transactions
    WHERE merchant IS NOT NULL
      AND merchant IS DISTINCT FROM 'Shell'
),
enriched AS (
    -- Attach accounts attributes
    SELECT s.*, d.account_type
    FROM valid s
    LEFT JOIN accounts d
        ON s.account_id = d.id
),
enriched_transactions AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        p.account_type AS account_account_type,
        p.balance,
        p.opened_date
    FROM enriched s
    INNER JOIN accounts p
        ON s.account_id = p.id
    WHERE p.account_type NOT IN ('SAVINGS', 'CHECKING', 'CREDIT')
),
enriched_transactions_2 AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        b.opened_date AS account_opened_date,
        b.balance AS account_balance
    FROM enriched_transactions a
    LEFT JOIN accounts b
        ON a.account_id = b.id
),
enriched_transactions_3 AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        account_type,
        opened_date,
        account_opened_date,
        RANK() OVER (PARTITION BY account_id ORDER BY account_balance ASC) account_balance_rank,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY account_balance ASC, id ASC) event_seq
    FROM enriched_transactions_2
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN event_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_tier,
        DATE_TRUNC('YEAR', opened_date) AS year_start,
        SIZE(COLLECT_SET(account_type)) AS num_account_types,
        MAX(event_seq) FILTER (WHERE account_type = 'CHECKING') AS checking_max_event_seq
    FROM enriched_transactions_3
    GROUP BY 1, 2
)
SELECT
    event_seq_tier,
    year_start,
    num_account_types,
    checking_max_event_seq
FROM stats
ORDER BY event_seq_tier DESC NULLS FIRST
