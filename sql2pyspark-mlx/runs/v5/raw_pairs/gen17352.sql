WITH cleaned AS (
    SELECT
        *,
        HOUR(txn_time) AS txn_time_hour,
        COALESCE(merchant, 'UNKNOWN') AS merchant_clean
    FROM transactions
    WHERE txn_time >= DATE '2023-04-27'
      AND account_id IN (22, 25)
),
valid AS (
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        COALESCE(txn_time_hour, 0) AS txn_time_hour_filled,
        DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_time_label
    FROM cleaned
    WHERE merchant IS NOT NULL
      AND merchant IS NOT DISTINCT FROM 'Shell'
),
latest AS (
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time_hour_filled, id) = 1
),
enriched AS (
    SELECT
        a.id,
        a.account_id,
        a.txn_type,
        a.txn_time,
        a.merchant,
        a.is_flagged,
        a.txn_time_hour_filled,
        a.txn_time_label,
        d.balance,
        d.status,
        d.opened_date
    FROM latest a
    INNER JOIN accounts d
        ON a.account_id = d.id
    WHERE d.account_type = 'LOAN'
),
ranked AS (
    SELECT
        id,
        txn_type,
        merchant,
        is_flagged,
        txn_time_hour_filled,
        status,
        opened_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, status ORDER BY balance DESC NULLS FIRST, id) AS dedup_rank
        FROM enriched
    ) x
    WHERE dedup_rank = 1
),
pivoted AS (
    SELECT *
    FROM (
        SELECT
            txn_type,
            status,
            merchant,
            txn_time_hour_filled
        FROM ranked
    ) src
    PIVOT (
        MAX(txn_time_hour_filled) AS value_sum,
        COUNT(1) AS cnt
        FOR merchant IN ('7-Eleven' AS v_7_eleven, 'Starbucks' AS starbucks, 'Amazon' AS amazon)
    )
)
SELECT
    txn_type,
    status,
    v_7_eleven_value_sum,
    v_7_eleven_cnt,
    starbucks_value_sum,
    starbucks_cnt
FROM pivoted
ORDER BY status DESC NULLS FIRST;
