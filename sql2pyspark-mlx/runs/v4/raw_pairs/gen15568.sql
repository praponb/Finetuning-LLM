WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN amount >= 4500 THEN 'High' WHEN amount >= 1500 THEN 'Medium' ELSE 'Low' END AS amount_tier,
        COALESCE(merchant, 'NONE') AS merchant_filled
    FROM transactions
    WHERE merchant IS NOT NULL
),
joined AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        a.account_type,
        a.status,
        a.opened_date
    FROM filtered src
    LEFT JOIN accounts a
        ON src.account_id = a.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY account_id, status ORDER BY amount ASC NULLS FIRST) AS amount_cume_dist,
        AVG(amount) OVER (ORDER BY CAST(txn_time AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_avg_30d
    FROM joined
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            account_id,
            txn_type,
            amount_avg_30d
        FROM metrics
    ) src
    PIVOT (
        SUM(amount_avg_30d)
        FOR txn_type IN ('REFUND', 'CREDIT', 'FEE')
    )
)
SELECT
    s.account_id,
    s.REFUND,
    s.CREDIT,
    s.FEE,
    (SELECT SUM(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS sum_transaction_amount
FROM by_type s
ORDER BY sum_transaction_amount ASC NULLS LAST;
