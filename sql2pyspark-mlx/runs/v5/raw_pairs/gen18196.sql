WITH with_dims AS (
    SELECT
        src.id,
        src.account_id,
        src.txn_type,
        src.amount,
        src.txn_time,
        a.balance
    FROM transactions src
    INNER JOIN accounts a
        ON src.account_id = a.id
),
gaps AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_dims
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC NULLS FIRST, id) AS rn
        FROM sessionized
    ) ranked
    WHERE rn = 1
),
metrics AS (
    SELECT
        *,
        SUM(new_group_flag) OVER w_part AS new_group_flag_sum_30d,
        LAG(island_id, 2, 0) OVER (PARTITION BY txn_type ORDER BY new_group_flag, id) AS prev_island_id,
        PERCENT_RANK() OVER (PARTITION BY txn_type, account_id ORDER BY amount DESC NULLS LAST) AS amount_percent_rank,
        STDDEV_SAMP(balance) OVER w_part_2 AS account_id_stddev_samp_balance,
        ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 4) AS amount_share
    FROM latest
    WINDOW w_part AS (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW), w_part_2 AS (PARTITION BY account_id)
),
grouped AS (
    SELECT
        CASE WHEN new_group_flag >= 1 THEN 'HIGH' ELSE 'LOW' END AS new_group_flag_tier,
        account_id,
        ROUND(AVG(amount), 2) AS avg_amount,
        COUNT(DISTINCT txn_type) AS distinct_txn_types,
        MIN(prev_island_id) AS min_prev_island_id,
        COUNT(DISTINCT account_id) AS distinct_account_ids,
        SUM(balance) FILTER (WHERE txn_type = 'CREDIT') AS credit_total_balance,
        COUNT(*) AS cnt,
        GROUPING_ID() AS gid
    FROM metrics
    GROUP BY ROLLUP (CASE WHEN new_group_flag >= 1 THEN 'HIGH' ELSE 'LOW' END, account_id)
)
SELECT
    new_group_flag_tier,
    avg_amount,
    distinct_txn_types,
    min_prev_island_id,
    distinct_account_ids,
    credit_total_balance,
    gid
FROM grouped
ORDER BY avg_amount;
