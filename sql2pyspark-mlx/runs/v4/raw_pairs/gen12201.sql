WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(amount / SUM(amount) OVER (PARTITION BY merchant), 4) AS amount_share,
        ROW_NUMBER() OVER (PARTITION BY account_id, txn_type ORDER BY txn_time DESC, id) AS row_seq
    FROM transactions
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        p.status,
        p.balance
    FROM metrics e
    LEFT JOIN accounts p
        ON e.account_id = p.id
),
with_dims_transactions AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        a.opened_date,
        a.account_type,
        a.status AS account_status
    FROM with_dims s
    LEFT JOIN accounts a
        ON s.account_id = a.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        amount_share,
        row_seq,
        status,
        balance,
        opened_date,
        account_type,
        account_status,
        SUM(amount) OVER (PARTITION BY account_status ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        LAG(amount_share, 2) OVER (PARTITION BY status ORDER BY txn_time, id) AS prev_amount_share,
        COUNT(amount_share) OVER (PARTITION BY merchant, status) AS merchant_count_amount_share,
        ROW_NUMBER() OVER (PARTITION BY account_status, account_id ORDER BY amount, id) AS seq_no
    FROM with_dims_transactions
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) AS frozen_count,
    SUM(CASE WHEN merchant = 'Starbucks' THEN row_seq ELSE 0 END) AS starbucks_row_seq,
    COUNT_IF(merchant = '7-Eleven') AS 7_eleven_events,
    PERCENTILE_APPROX(cumulative_amount, ARRAY(0.25, 0.5, 0.75)) AS cumulative_amount_quartiles,
    MAX(txn_time) AS ended_at,
    COUNT(*) FILTER (WHERE txn_type = 'DEBIT') AS debit_rows
FROM enriched
GROUP BY CUBE (DATE_TRUNC('YEAR', txn_time))
ORDER BY frozen_count DESC NULLS LAST;
