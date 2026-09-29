WITH valid AS (
    SELECT *, LOWER(merchant) AS merchant_lower
    FROM transactions
    WHERE merchant IS DISTINCT FROM 'Amazon'
      AND txn_time > TIMESTAMP '2023-07-18 00:00:00'
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS event_seq,
        MAX(amount) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_max_7d,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_sum_30d,
        AVG(amount) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_avg_6_prior
    FROM valid
),
scored AS (
    SELECT
        *,
        SUM(event_seq) OVER (PARTITION BY merchant, account_id) AS merchant_sum_event_seq,
        ROUND(amount_max_7d / SUM(amount_max_7d) OVER (PARTITION BY txn_type), 4) AS amount_max_7d_share,
        NTILE(10) OVER (PARTITION BY account_id ORDER BY event_seq DESC, id) AS event_seq_bucket
    FROM enriched
),
enriched_transactions AS (
    SELECT
        src.*,
        d.status,
        d.account_type
    FROM scored src
    INNER JOIN accounts d
        ON src.account_id = d.id
    WHERE d.balance < 66000
),
joined AS (
    SELECT
        a.id,
        a.account_id,
        a.txn_type,
        a.txn_time,
        a.merchant_lower,
        a.event_seq,
        a.amount_max_7d,
        a.amount_avg_6_prior,
        a.amount_max_7d_share,
        b.status AS account_status,
        b.opened_date
    FROM enriched_transactions a
    LEFT JOIN accounts b
        ON a.account_id = b.id
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_status ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_status ORDER BY txn_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM joined
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY account_status ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    s.id,
    s.account_id,
    s.merchant_lower,
    s.event_seq,
    s.amount_max_7d,
    s.amount_avg_6_prior,
    s.amount_max_7d_share,
    s.opened_date,
    (SELECT MAX(x.amount) FROM transactions x WHERE x.account_id = s.account_id) AS max_transaction_amount
FROM islands s;
