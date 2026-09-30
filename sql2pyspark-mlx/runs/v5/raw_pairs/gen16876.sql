WITH metrics AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        LEAD(amount, 1, 0) OVER w_part AS next_amount,
        AVG(amount) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_avg_5,
        LEAD(amount, 1, 0) OVER w_running AS next_amount_2,
        ROW_NUMBER() OVER w_part_2 AS event_seq,
        ROUND(amount / SUM(amount) OVER (PARTITION BY merchant, account_id), 4) AS amount_share
    FROM transactions
    WINDOW w_part AS (ORDER BY txn_time ASC, id ASC), w_running AS (PARTITION BY account_id ORDER BY txn_time ASC, id ASC), w_part_2 AS (PARTITION BY txn_type, merchant ORDER BY amount DESC, id ASC)
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount_share DESC, id ASC) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
),
latest AS (
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        next_amount,
        amount_avg_5,
        amount_share,
        latest_rank
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY latest_rank DESC, id ASC) = 1
),
ranked_transactions AS (
    SELECT
        id,
        txn_type,
        merchant
    FROM latest
    QUALIFY RANK() OVER (PARTITION BY txn_type, merchant ORDER BY amount DESC NULLS FIRST) = 1
),
filtered AS (
    SELECT
        *,
        LOWER(txn_type) AS txn_type_lower,
        COALESCE(merchant, 'DIRECT') AS resolved_merchant
    FROM ranked_transactions
    WHERE merchant IS NOT NULL
      AND merchant IS DISTINCT FROM '7-Eleven'
),
base AS (
    SELECT
        id,
        txn_type,
        txn_type_lower,
        resolved_merchant,
        INITCAP(TRIM(merchant)) AS merchant_title
    FROM filtered
    WHERE merchant IS NOT NULL
      AND merchant IS NOT NULL
)
SELECT
    txn_type,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE txn_type = 'CREDIT') AS credit_rows
FROM base
GROUP BY txn_type
HAVING cnt < 1000
