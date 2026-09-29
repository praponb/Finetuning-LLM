WITH filtered AS (
    SELECT
        *,
        CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END AS is_fee,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'DIRECT') AS resolved_merchant
    FROM transactions
    WHERE txn_time > DATE '2023-07-05'
      AND merchant IS NOT NULL
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY is_flagged_flag DESC NULLS LAST, id) AS rank_in_group
    FROM filtered
    QUALIFY rank_in_group = 1
),
with_gaps AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM ranked
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    CASE WHEN is_fee >= 62 THEN 'HIGH' ELSE 'LOW' END AS is_fee_tier,
    COUNT(*) FILTER (WHERE merchant = '7-Eleven') AS 7_eleven_rows,
    MAX(is_new_session) AS max_is_new_session,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END), 0), 2) AS amount_per_debit,
    ROUND(SUM(session_id) / NULLIF(COUNT(CASE WHEN merchant = 'Grab' THEN 1 END), 0), 2) AS session_id_per_grab,
    APPROX_PERCENTILE(amount, 0.9) AS amount_p90,
    COUNT(CASE WHEN merchant = 'Starbucks' THEN 1 END) AS starbucks_count
FROM islands
GROUP BY CASE WHEN is_fee >= 62 THEN 'HIGH' ELSE 'LOW' END
ORDER BY max_is_new_session DESC
