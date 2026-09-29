SELECT
    CASE WHEN balance >= 76000 THEN 'HIGH' ELSE 'LOW' END AS balance_tier,
    SIZE(COLLECT_SET(status)) AS num_status,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    COUNT_IF(status = 'FROZEN') AS frozen_events
FROM accounts
GROUP BY balance_tier
ORDER BY num_status ASC;
