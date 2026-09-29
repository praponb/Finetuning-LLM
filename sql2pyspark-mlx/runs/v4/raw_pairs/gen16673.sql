SELECT
    status,
    method,
    CASE WHEN method IN ('promptpay', 'paypal') THEN 'tier_1' ELSE 'other' END method_group,
    COUNT(amount) amount_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values
FROM payments
WHERE paid_at < '2023-09-30'
GROUP BY 1, 2, 3
HAVING SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END) >= 53
ORDER BY method_group
