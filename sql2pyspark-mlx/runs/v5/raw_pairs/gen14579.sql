WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(amount) amount_whole,
        COALESCE(merchant, 'DIRECT') resolved_merchant
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-09-12 17:00:00'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY merchant, account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_amount,
        RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC NULLS FIRST) amount_rank,
        DENSE_RANK() OVER (ORDER BY amount DESC NULLS FIRST) amount_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY merchant ORDER BY amount_whole NULLS LAST) amount_whole_percent_rank,
        NTILE(3) OVER (PARTITION BY account_id, txn_type ORDER BY amount DESC, id) amount_bucket
    FROM valid
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM enriched
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM islands
),
islands_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM gaps_2
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(burst_id) OVER (PARTITION BY account_id ORDER BY burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_burst_id,
        AVG(amount_dense_rank) OVER (PARTITION BY txn_type ORDER BY burst_id_2, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) amount_dense_rank_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount_rank, id) event_seq
    FROM islands_transactions
)
SELECT
    id,
    account_id,
    txn_type,
    amount,
    is_flagged,
    amount_whole,
    resolved_merchant,
    amount_rank,
    amount_bucket,
    CASE WHEN txn_type RLIKE '^(CREDIT|REFUND)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS txn_type_zone
FROM metrics
ORDER BY txn_type DESC, id DESC
