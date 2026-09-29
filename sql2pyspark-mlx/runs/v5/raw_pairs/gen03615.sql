WITH filtered AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
    FROM accounts
    WHERE customer_id IN (4, 12, 14, 21)
)
SELECT
    d.segment,
    a.account_type,
    COUNT(*) AS rows_total,
    SUM(a.days_since_opened_date) AS total_days_since_opened_date
FROM filtered a
JOIN customers d ON a.customer_id = d.id
GROUP BY CUBE (d.segment, a.account_type)
ORDER BY segment DESC;
