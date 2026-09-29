WITH metrics AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY amount DESC) AS amount_cume_dist,
        MAX(amount) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_max_7d
    FROM transactions
),
enriched AS (
    SELECT e.*, p.status
    FROM metrics e
    LEFT JOIN accounts p
        ON e.account_id = p.id
),
with_dims AS (
    SELECT
        e.*,
        p.account_type,
        p.opened_date
    FROM enriched e
    LEFT JOIN accounts p
        ON e.account_id = p.id
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.amount_cume_dist,
    s.amount_max_7d,
    s.account_type,
    s.opened_date,
    CASE WHEN txn_type RLIKE '^(CREDIT|FEE)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS txn_type_zone,
    (SELECT SUM(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS sum_transaction_amount
FROM with_dims s
ORDER BY id DESC
LIMIT 20;
