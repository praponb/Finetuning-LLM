WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        UPPER(txn_type) AS txn_type_upper
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 6 AND 9
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    MAX(txn_time) AS ended_at,
    COUNT_IF(merchant = 'Grab') AS grab_events,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types,
    MAX(txn_time) AS ended_at_2,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    SIZE(COLLECT_SET(merchant)) AS num_merchants,
    GROUPING(DATE_TRUNC('YEAR', txn_time)) AS grouping_level
FROM filtered
GROUP BY CUBE (DATE_TRUNC('YEAR', txn_time))
HAVING num_merchants > 1
ORDER BY grab_events NULLS LAST;
