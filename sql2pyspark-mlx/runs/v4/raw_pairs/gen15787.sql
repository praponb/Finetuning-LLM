WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        status,
        LENGTH(status) AS status_length,
        COALESCE(status, 'UNKNOWN') AS resolved_status
    FROM accounts
    WHERE balance < 43000
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY status_length DESC NULLS LAST, id) AS dedup_rank
        FROM filtered
    ) t
    WHERE dedup_rank = 1
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.merchant
    FROM ranked s
    JOIN LATERAL (
            SELECT c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Multi-level aggregation
SELECT
    status,
    CASE WHEN status_length >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_length_tier,
    SUM(CASE WHEN status = 'FROZEN' THEN status_length ELSE 0 END) AS frozen_status_length,
    COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) AS closed_count,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
    PERCENTILE_APPROX(status_length, ARRAY(0.25, 0.5, 0.75)) AS status_length_quartiles,
    GROUPING(status) AS gid
FROM with_last
GROUP BY GROUPING SETS ((status, CASE WHEN status_length >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END), (status))
HAVING closed_count >= 1
ORDER BY status DESC;
