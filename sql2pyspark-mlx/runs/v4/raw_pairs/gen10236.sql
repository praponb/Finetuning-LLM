WITH enriched AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        d.opened_date,
        d.balance
    FROM transactions e
    INNER JOIN accounts d
        ON e.account_id = d.id
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM enriched
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        opened_date,
        balance,
        is_new_session,
        session_no,
        NTILE(4) OVER (PARTITION BY account_id, merchant ORDER BY amount, id) AS amount_bucket,
        LAST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY account_id, txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance
    FROM sessionized
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session_2
    FROM scored
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session_2) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
enriched_transactions AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY merchant, txn_type ORDER BY txn_time DESC, id) AS event_seq,
        LAST_VALUE(balance, TRUE) OVER w_grp AS last_known_balance_2
    FROM islands
    WINDOW w_grp AS (ORDER BY is_new_session_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        opened_date,
        balance,
        is_new_session,
        session_no,
        amount_bucket,
        last_known_balance,
        is_new_session_2,
        island_id,
        event_seq,
        last_known_balance_2,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY account_id), 4) AS event_seq_share,
        LAG(is_new_session, 2) OVER w_ordered AS prev_is_new_session
    FROM enriched_transactions
    WINDOW w_ordered AS (ORDER BY txn_time, id)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN is_new_session_2 >= 1 THEN 'HIGH' ELSE 'LOW' END AS is_new_session_2_tier,
    CASE WHEN prev_is_new_session >= 1 THEN 'HIGH' ELSE 'LOW' END AS prev_is_new_session_tier,
    COUNT(CASE WHEN merchant = 'Starbucks' THEN 1 END) AS starbucks_count,
    PERCENTILE_APPROX(amount_bucket, 0.95) AS amount_bucket_p95,
    MIN(island_id) AS min_island_id,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list
FROM metrics
GROUP BY CASE WHEN is_new_session_2 >= 1 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN prev_is_new_session >= 1 THEN 'HIGH' ELSE 'LOW' END;
