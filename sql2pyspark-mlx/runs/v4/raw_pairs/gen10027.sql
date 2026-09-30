WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END AS is_fee,
        COALESCE(merchant, 'NONE') AS resolved_merchant
    FROM transactions
    WHERE merchant IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(is_fee, 0) AS is_fee_filled
    FROM sessions_transactions
    WHERE txn_time >= TIMESTAMP '2023-10-06 00:00:00'
      AND merchant IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY merchant ORDER BY amount ASC, id ASC) AS amount_bucket,
        AVG(is_fee_filled) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS is_fee_filled_avg_5_prior,
        LAST_VALUE(is_fee_filled) IGNORE NULLS OVER (PARTITION BY merchant ORDER BY session_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_fee_filled
    FROM filtered
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(last_known_is_fee_filled) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last_known_is_fee_filled,
        SUM(last_known_is_fee_filled) OVER (PARTITION BY account_id, merchant ORDER BY session_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last_known_is_fee_filled,
        AVG(is_new_burst) OVER (PARTITION BY account_id) AS account_id_avg_is_new_burst
    FROM scored
)
-- Grouped metrics
SELECT
    CASE WHEN is_fee >= 78 THEN 'LARGE' ELSE 'SMALL' END AS is_fee_tier,
    session_no,
    ROUND(AVG(is_new_burst), 2) AS avg_is_new_burst,
    PERCENTILE_APPROX(is_fee_filled, 0.5) AS is_fee_filled_p50,
    COUNT(*) AS event_count
FROM enriched
GROUP BY CUBE (CASE WHEN is_fee >= 78 THEN 'LARGE' ELSE 'SMALL' END, session_no)
ORDER BY avg_is_new_burst ASC NULLS LAST;
