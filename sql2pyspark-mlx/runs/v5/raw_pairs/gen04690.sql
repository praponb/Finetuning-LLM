WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        LENGTH(merchant) merchant_length,
        COALESCE(merchant, 'OTHER') resolved_merchant
    FROM transactions
    WHERE txn_time >= TO_DATE('2023-08-03')
      AND merchant IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_amount,
        MAX(merchant_length) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) merchant_length_max_30d,
        CUME_DIST() OVER (PARTITION BY account_id, txn_type ORDER BY merchant_length DESC) merchant_length_cume_dist,
        LAG(amount, 1, 0) OVER (PARTITION BY account_id ORDER BY amount, id) prev_amount
    FROM base
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.txn_type,
        s.amount,
        s.is_flagged,
        s.merchant_length,
        s.resolved_merchant,
        s.merchant_length_max_30d,
        a.opened_date,
        a.status
    FROM scored s
    LEFT JOIN accounts a
        ON s.account_id = a.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT txn_type, account_id
    FROM enriched
    EXCEPT
    SELECT txn_type, account_id
    FROM transactions
    WHERE txn_time < '2023-05-30'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.txn_type = k.txn_type AND s.account_id = k.account_id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        amount,
        is_flagged,
        merchant_length,
        resolved_merchant,
        opened_date,
        status,
        MIN(merchant_length) OVER (PARTITION BY txn_type, account_id) txn_type_min_merchant_length,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY merchant_length_max_30d NULLS FIRST) merchant_length_max_30d_dense_rank,
        COUNT(merchant_length_max_30d) OVER (PARTITION BY account_id, txn_type) account_id_count_merchant_length_max_30d
    FROM reconciled
    WHERE amount >= 3700
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        status,
        COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) AS frozen_count,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE status = 'FROZEN') AS frozen_rows,
        GROUPING(status) AS gid
    FROM metrics
    GROUP BY ROLLUP (status)
)
SELECT
    status,
    frozen_count,
    distinct_ids,
    gid,
    CASE WHEN status RLIKE '^(CLOSED|FROZEN)$' THEN 'GROUP_B' WHEN status RLIKE '^(OPEN)$' THEN 'CORE' ELSE 'GROUP_A' END AS status_zone
FROM totals
ORDER BY distinct_ids DESC NULLS LAST
