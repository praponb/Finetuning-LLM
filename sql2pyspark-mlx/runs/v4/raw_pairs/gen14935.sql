WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        AVG(balance) OVER w_ordered AS balance_avg_6,
        LAG(balance, 2) OVER w_recent AS prev_balance
    FROM accounts
    WINDOW w_ordered AS (PARTITION BY status ORDER BY balance, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY opened_date, id)
)
-- Grouped metrics
SELECT
    status,
    COUNT(DISTINCT status) AS distinct_status,
    ROUND(SUM(balance_avg_6) / NULLIF(COUNT(CASE WHEN status = 'FROZEN' THEN 1 END), 0), 2) AS balance_avg_6_per_frozen,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    ROUND(SUM(balance_avg_6) / NULLIF(COUNT(CASE WHEN status = 'CLOSED' THEN 1 END), 0), 2) AS balance_avg_6_per_closed
FROM with_windows
GROUP BY status
