SELECT
    id,
    method,
    CASE WHEN amount >= 1260 THEN 'High' WHEN amount >= 520 THEN 'Medium' ELSE 'Low' END AS amount_tier,
    TRIM(method) AS clean_method
FROM payments
WHERE amount >= 970 AND method IN ('promptpay', 'card')
ORDER BY id DESC;
