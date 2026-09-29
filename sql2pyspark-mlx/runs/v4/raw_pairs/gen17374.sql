SELECT
    CASE WHEN amount >= 3000 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    COUNT(CASE WHEN txn_type = 'FEE' THEN 1 END) AS fee_count,
    STDDEV_SAMP(amount) AS amount_stddev
FROM transactions
GROUP BY CASE WHEN amount >= 3000 THEN 'HIGH' ELSE 'LOW' END;
