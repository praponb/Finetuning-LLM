WITH filtered AS (
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        LENGTH(txn_type) AS txn_type_length,
        COALESCE(merchant, 'N/A') AS resolved_merchant
    FROM transactions
    WHERE account_id IN (11, 27, 32, 37)
      AND merchant IS NOT NULL
),
joined AS (
    SELECT
        e.*,
        d.opened_date,
        d.account_type
    FROM filtered e
    INNER JOIN accounts d
        ON e.account_id = d.id
    WHERE d.customer_id IN (5, 13)
),
gaps AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM joined
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
scored AS (
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        txn_type_length,
        resolved_merchant,
        account_type,
        is_new_session,
        FIRST_VALUE(account_type) IGNORE NULLS OVER (PARTITION BY account_id, txn_type ORDER BY burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_account_type,
        NTILE(3) OVER w_part AS txn_type_length_bucket,
        LEAD(txn_type_length, 1) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS next_txn_type_length,
        RANK() OVER (PARTITION BY account_id ORDER BY is_new_session DESC NULLS FIRST) AS is_new_session_rank
    FROM islands
    WINDOW w_part AS (ORDER BY txn_type_length DESC, id)
),
flagged AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session_2
    FROM scored
),
sessions_transactions AS (
    SELECT *, SUM(is_new_session_2) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    CASE WHEN txn_type_length_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS txn_type_length_bucket_tier,
    AVG(txn_type_length_bucket) FILTER (WHERE account_type = 'CREDIT') AS credit_avg_txn_type_length_bucket,
    APPROX_PERCENTILE(txn_type_length_bucket, 0.9) AS txn_type_length_bucket_p90,
    ROUND(SUM(session_id) / NULLIF(COUNT(CASE WHEN account_type = 'CHECKING' THEN 1 END), 0), 2) AS session_id_per_checking,
    COUNT(*) AS cnt
FROM sessions_transactions
GROUP BY 1
HAVING cnt > 1
ORDER BY cnt DESC NULLS LAST;
