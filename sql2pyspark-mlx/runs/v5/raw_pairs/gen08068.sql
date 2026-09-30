WITH metrics AS (
    SELECT
        *,
        MIN(amount) OVER w_hist running_min_amount,
        COUNT(amount) OVER w_hist running_count_amount,
        MAX(amount) OVER w_hist running_max_amount
    FROM transactions
    WINDOW w_hist AS (PARTITION BY txn_type, account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        a.*,
        d.balance,
        d.opened_date
    FROM metrics a
    LEFT JOIN accounts d
        ON a.account_id = d.id
),
with_dims AS (
    SELECT
        src.id,
        src.account_id,
        src.txn_time,
        src.running_min_amount,
        src.running_count_amount,
        src.running_max_amount,
        src.balance,
        src.opened_date,
        d.status
    FROM enriched src
    INNER JOIN accounts d
        ON src.account_id = d.id
),
gaps AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY status ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY status ORDER BY txn_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_dims
),
islands AS (
    SELECT
        id,
        txn_time,
        running_min_amount,
        opened_date,
        status,
        SUM(new_group_flag) OVER (PARTITION BY status ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
SELECT
    id,
    txn_time,
    running_min_amount,
    status,
    session_no
FROM islands
