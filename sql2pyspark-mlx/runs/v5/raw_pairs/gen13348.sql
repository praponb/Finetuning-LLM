WITH base AS (
    SELECT
        *,
        CASE WHEN txn_type IN ('FEE', 'CREDIT') THEN 'Core' ELSE 'Other' END txn_type_group,
        CASE WHEN merchant IN ('Shell', 'Starbucks') THEN 'Tier 1' ELSE 'Other' END merchant_group
    FROM transactions
    WHERE txn_time > TO_DATE('2023-08-10')
      AND merchant IS DISTINCT FROM 'Grab'
),
with_dims AS (
    SELECT e.*, b.opened_date
    FROM base e
    LEFT JOIN accounts b
        ON e.account_id = b.id
),
flagged AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_dims
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_dims_transactions AS (
    SELECT
        e.id,
        e.account_id,
        e.txn_type,
        e.txn_time,
        e.new_group_flag,
        e.burst_id,
        b.opened_date AS account_opened_date,
        b.balance
    FROM islands e
    LEFT JOIN accounts b
        ON e.account_id = b.id
    WHERE b.customer_id IN (5, 19, 21, 37)
),
scored AS (
    SELECT
        id,
        account_id,
        txn_type,
        new_group_flag,
        account_opened_date,
        balance,
        ROW_NUMBER() OVER (PARTITION BY txn_type, account_id ORDER BY burst_id ASC, id ASC) event_seq,
        AVG(new_group_flag) OVER (PARTITION BY account_id, txn_type ORDER BY balance ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) new_group_flag_avg_5_prior,
        AVG(new_group_flag) OVER (PARTITION BY account_id, txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) new_group_flag_avg_6
    FROM with_dims_transactions
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY balance ASC NULLS FIRST, id ASC) AS dedup_rank
    FROM scored
    QUALIFY dedup_rank = 1
),
matched_keys AS (
    SELECT account_id, id
    FROM latest
    INTERSECT ALL
    SELECT account_id, id
    FROM transactions
    WHERE account_id = 32
),
reconciled AS (
    SELECT s.*
    FROM latest s
    JOIN matched_keys k ON s.account_id = k.account_id AND s.id = k.id
)
SELECT *
FROM (
    SELECT
        account_id,
        txn_type,
        new_group_flag
    FROM reconciled
) src
PIVOT (
    SUM(new_group_flag) AS amount,
    COUNT(1) AS n
    FOR txn_type IN ('FEE' AS fee, 'DEBIT' AS debit)
)
