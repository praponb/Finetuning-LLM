SELECT
    id,
    is_flagged,
    merchant,
    amount,
    CASE WHEN amount >= 4400 THEN 'High' WHEN amount >= 1800 THEN 'Medium' ELSE 'Low' END AS amount_tier,
    CAST(is_flagged AS INT) AS is_flagged_flag
FROM transactions
WHERE account_id IN (6, 11, 24);
