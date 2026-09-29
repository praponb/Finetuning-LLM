WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY amount DESC NULLS LAST, id ASC) AS rank_in_group
    FROM transactions
    QUALIFY rank_in_group = 1
),
scored AS (
    SELECT
        *,
        LEAD(amount) OVER w_part AS next_amount,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM latest
    WHERE rank_in_group >= 1
    WINDOW w_part AS (PARTITION BY merchant ORDER BY amount ASC, id ASC)
),
remaining_keys AS (
    SELECT merchant, account_id
    FROM scored
    INTERSECT
    SELECT merchant, account_id
    FROM transactions
    WHERE merchant = 'Amazon'
),
reconciled AS (
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.merchant = k.merchant AND s.account_id = k.account_id
),
enriched AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        rank_in_group,
        next_amount,
        event_seq,
        SUM(next_amount) OVER (PARTITION BY account_id, txn_type ORDER BY CAST(txn_time AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS next_amount_sum_30d,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY merchant), 4) AS event_seq_share,
        RANK() OVER (PARTITION BY merchant ORDER BY event_seq ASC) AS event_seq_rank,
        ROUND(amount / SUM(amount) OVER (PARTITION BY merchant), 4) AS amount_share
    FROM reconciled
),
base AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        is_flagged,
        next_amount,
        event_seq,
        next_amount_sum_30d,
        CAST(rank_in_group AS DOUBLE) AS rank_in_group_str
    FROM enriched
    WHERE event_seq BETWEEN 4 AND 5
),
daily AS (
    SELECT
        account_id,
        txn_type,
        MAX(amount) AS max_amount,
        MAX(next_amount) AS max_next_amount,
        COUNT(*) FILTER (WHERE txn_type = 'CREDIT') AS credit_rows
    FROM base
    GROUP BY account_id, txn_type
)
SELECT
    s.account_id,
    s.txn_type,
    CASE WHEN txn_type RLIKE '^(DEBIT|REFUND)$' THEN 'CORE' WHEN txn_type RLIKE '^(CREDIT|FEE)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS txn_type_zone,
    (SELECT AVG(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS avg_transaction_amount
FROM daily s;
