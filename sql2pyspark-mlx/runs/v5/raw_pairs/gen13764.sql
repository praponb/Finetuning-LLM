WITH FilteredAccounts AS (
    SELECT *
    FROM accounts
    WHERE customer_id IN (24, 31, 36, 39)
),
agg AS (
    SELECT CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END opened_cohort, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values, MAX(balance) - MIN(balance) balance_range
    FROM FilteredAccounts
    GROUP BY CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT opened_cohort, status_values, balance_range
FROM agg
WHERE balance_range >= 36
