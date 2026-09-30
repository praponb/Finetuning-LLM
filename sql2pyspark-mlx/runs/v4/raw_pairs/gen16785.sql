SELECT
    id,
    merchant,
    amount,
    CASE WHEN amount >= 4300 THEN 'High' WHEN amount >= 1600 THEN 'Medium' ELSE 'Low' END AS amount_tier,
    INITCAP(TRIM(merchant)) AS merchant_title
FROM transactions
WHERE account_id IN (8, 22, 38);
