WITH gaps AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
remaining_keys AS (
    SELECT id
    FROM islands
    EXCEPT
    SELECT id
    FROM transactions
    WHERE NOT is_flagged
),
kept_rows AS (
    SELECT s.*
    FROM islands s
    INNER JOIN remaining_keys k ON s.id = k.id
),
scored AS (
    SELECT
        account_id,
        merchant,
        burst_id,
        PERCENT_RANK() OVER (PARTITION BY merchant, account_id ORDER BY burst_id) AS burst_id_percent_rank
    FROM kept_rows
),
enriched AS (
    SELECT s.*, d.balance
    FROM scored s
    LEFT JOIN accounts d
        ON s.account_id = d.id
),
enriched_transactions AS (
    SELECT
        account_id,
        merchant,
        burst_id_percent_rank,
        balance,
        AVG(burst_id) OVER w_ordered AS account_id_avg_burst_id
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY account_id)
)
SELECT
    merchant,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
    COUNT(CASE WHEN merchant = 'Shell' THEN 1 END) AS shell_count,
    COUNT(CASE WHEN merchant = 'Amazon' THEN 1 END) AS amazon_count
FROM enriched_transactions
GROUP BY merchant
HAVING COUNT(CASE WHEN merchant = 'Amazon' THEN 1 END) < 1000
ORDER BY amazon_count
