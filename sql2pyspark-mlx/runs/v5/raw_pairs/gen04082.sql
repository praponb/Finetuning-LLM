WITH with_dims AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        p.opened_date,
        p.status
    FROM transactions e
    INNER JOIN accounts p
        ON e.account_id = p.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount NULLS LAST, id) AS rank_in_group
    FROM with_dims
    QUALIFY rank_in_group = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT merchant, txn_type
    FROM ranked
    EXCEPT ALL
    SELECT merchant, txn_type
    FROM transactions
    WHERE account_id IN (28, 36)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.merchant = k.merchant AND s.txn_type = k.txn_type
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(amount) OVER (PARTITION BY merchant, status ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_max_90d,
        PERCENT_RANK() OVER (PARTITION BY txn_type ORDER BY amount) AS amount_percent_rank,
        STDDEV_SAMP(amount) OVER w_hist AS account_id_stddev_samp_amount,
        MAX(amount) OVER w_ordered AS amount_max_90d_2,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) AS amount_share
    FROM kept_rows
    WHERE account_id IN (6, 10, 31)
    WINDOW w_hist AS (PARTITION BY account_id, status), w_ordered AS (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW)
),
joined AS (
    -- Attach accounts attributes
    SELECT e.*, p.status AS account_status
    FROM scored e
    LEFT JOIN accounts p
        ON e.account_id = p.id
),
joined_transactions AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        p.balance,
        p.opened_date AS account_opened_date,
        p.account_type
    FROM joined src
    LEFT JOIN accounts p
        ON src.account_id = p.id
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        e.id,
        e.account_id,
        e.amount,
        e.txn_time,
        e.status,
        e.amount_max_90d,
        e.account_id_stddev_samp_amount,
        e.amount_max_90d_2,
        e.account_opened_date,
        e.account_type,
        b.status AS account_status_2,
        b.account_type AS account_account_type,
        b.balance AS account_balance
    FROM joined_transactions e
    LEFT JOIN accounts b
        ON e.account_id = b.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    status,
    metric_name,
    value
FROM (
    SELECT
        status,
        amount,
        account_balance
    FROM enriched
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric_name IN (amount, account_balance)
) u;
