WITH recent_accounts AS (
    SELECT *
    FROM accounts
    WHERE status <> 'CLOSED'
),
Agg AS (
    SELECT MONTH(opened_date) AS opened_month, SORT_ARRAY(COLLECT_SET(status)) AS status_list, AVG(balance) AS avg_balance
    FROM recent_accounts
    GROUP BY MONTH(opened_date)
)
SELECT opened_month, status_list
FROM Agg
WHERE avg_balance >= 42000
ORDER BY opened_month DESC
