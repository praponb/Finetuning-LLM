WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM transactions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
kept_keys AS (
    -- Set intersection on keys
    SELECT account_id, merchant
    FROM sessionized
    INTERSECT
    SELECT account_id, merchant
    FROM transactions
    WHERE account_id IN (1, 15, 31)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessionized s
    INNER JOIN kept_keys k ON s.account_id = k.account_id AND s.merchant = k.merchant
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        amount / NULLIF(burst_id, 0) amount_per_burst_id,
        CONCAT(txn_type, ' ', merchant) txn_type_merchant,
        COALESCE(merchant, 'UNKNOWN') resolved_merchant
    FROM reconciled
    WHERE account_id IN (5, 7, 9, 12)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        is_new_burst,
        burst_id,
        amount_per_burst_id,
        txn_type_merchant,
        resolved_merchant,
        SUM(amount) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) amount_sum_30d,
        DENSE_RANK() OVER (ORDER BY amount DESC NULLS FIRST) amount_dense_rank,
        MAX(amount_per_burst_id) OVER (PARTITION BY merchant) merchant_max_amount_per_burst_id
    FROM filtered
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    CASE WHEN amount_sum_30d >= 20791 THEN 'HIGH' ELSE 'LOW' END AS amount_sum_30d_tier,
    AVG(amount) FILTER (WHERE merchant = '7-Eleven') AS 7_eleven_avg_amount,
    COUNT(DISTINCT id) AS distinct_ids,
    RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', txn_time) ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_rank
FROM enriched
GROUP BY week_start, amount_sum_30d_tier
ORDER BY distinct_ids DESC NULLS FIRST
