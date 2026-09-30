WITH valid AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        TO_DATE(txn_time) txn_time_date,
        SUBSTRING(merchant, 1, 4) merchant_prefix
    FROM transactions
    WHERE txn_time >= TO_TIMESTAMP('2023-07-07 00:00:00')
      AND merchant IS NOT NULL
),
enriched AS (
    SELECT
        a.*,
        p.opened_date,
        p.account_type
    FROM valid a
    LEFT JOIN accounts p
        ON a.account_id = p.id
    WHERE p.customer_id = 10
),
enriched_transactions AS (
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY account_type, txn_type ORDER BY txn_time_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_amount,
        AVG(amount) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) amount_avg_90d,
        PERCENT_RANK() OVER (PARTITION BY account_type, merchant ORDER BY amount NULLS FIRST) amount_percent_rank
    FROM enriched
),
with_windows AS (
    SELECT
        *,
        SUM(cumulative_amount) OVER w_recent cumulative_amount_sum_90d,
        FIRST_VALUE(account_type, TRUE) OVER w_running first_known_account_type,
        AVG(cumulative_amount) OVER (PARTITION BY account_type ORDER BY amount_percent_rank, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) cumulative_amount_avg_5,
        FIRST_VALUE(amount_percent_rank, TRUE) OVER (PARTITION BY txn_type, merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_amount_percent_rank,
        LEAD(amount, 1) OVER (PARTITION BY account_type, account_id ORDER BY amount_percent_rank, id) next_amount
    FROM enriched_transactions
    WINDOW w_recent AS (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY txn_type, account_type ORDER BY amount_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    SELECT merchant, id
    FROM with_windows
    EXCEPT DISTINCT
    SELECT merchant, id
    FROM transactions
    WHERE merchant = 'Starbucks'
),
reconciled AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.merchant = k.merchant AND s.id = k.id
)
SELECT *
FROM (
    SELECT
        account_id,
        account_type,
        txn_type,
        amount_avg_90d
    FROM reconciled
) src
PIVOT (
    MAX(amount_avg_90d)
    FOR txn_type IN ('DEBIT', 'FEE')
)
