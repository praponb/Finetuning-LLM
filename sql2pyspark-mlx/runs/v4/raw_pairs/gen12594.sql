WITH scored AS (
    SELECT
        *,
        LAST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY txn_type, account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        AVG(amount) OVER w_recent AS amount_avg_7d
    FROM transactions
    WINDOW w_recent AS (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
),
scored_transactions AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        last_known_amount,
        amount_avg_7d,
        FIRST_VALUE(amount) IGNORE NULLS OVER w_part AS first_known_amount,
        SUM(amount) OVER w_part_2 AS amount_sum_30d
    FROM scored
    WINDOW w_part AS (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part_2 AS (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        *,
        AVG(last_known_amount) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS last_known_amount_avg_90d,
        COUNT(amount_avg_7d) OVER (PARTITION BY merchant, txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_avg_7d_count_7d,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM scored_transactions
    WINDOW w_grp AS (PARTITION BY txn_type ORDER BY amount, id)
),
ranked AS (
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id, txn_type ORDER BY amount_avg_7d DESC NULLS FIRST, id) = 1
)
SELECT *
FROM (
    SELECT
        merchant,
        txn_type,
        row_seq
    FROM ranked
) src
PIVOT (
    AVG(row_seq) AS total,
    COUNT(1) AS n
    FOR txn_type IN ('REFUND', 'FEE')
);
