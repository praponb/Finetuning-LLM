WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        FLOOR(amount) AS amount_whole
    FROM transactions
    WHERE txn_time >= TO_DATE('2023-12-31')
      AND merchant IS NOT NULL
      AND account_id = 18
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY txn_type, account_id ORDER BY txn_time, id) AS seq_no,
        COUNT(amount) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_count_30d,
        LEAD(amount, 2) OVER (PARTITION BY txn_type ORDER BY amount_whole, id) AS next_amount
    FROM base
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        d.balance,
        d.opened_date
    FROM enriched a
    JOIN accounts d
        ON a.account_id = d.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(amount_whole) OVER w_ordered AS amount_whole_count_7d,
        LEAD(seq_no, 1) OVER w_recent AS next_seq_no,
        MAX(amount_whole) OVER w_ordered AS amount_whole_max_7d,
        STDDEV_SAMP(amount_count_30d) OVER (PARTITION BY account_id) AS account_id_stddev_samp_amount_count_30d
    FROM with_dims
    WINDOW w_ordered AS (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY opened_date, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        txn_type,
        balance,
        next_amount,
        amount
    FROM metrics
) src
UNPIVOT (
    reading FOR metric_name IN (balance, next_amount, amount)
);
