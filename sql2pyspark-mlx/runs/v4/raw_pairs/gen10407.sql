SELECT
    CASE WHEN segment IN ('Corporate', 'Consumer') THEN 'tier_1' ELSE 'other' END AS segment_group,
    SUM(credit_limit) AS total_credit_limit,
    COUNT(*) AS cnt,
    ROUND(SUM(credit_limit) * 100.0 / SUM(SUM(credit_limit)) OVER (), 2) AS pct_of_total
FROM customers
GROUP BY CASE WHEN segment IN ('Corporate', 'Consumer') THEN 'tier_1' ELSE 'other' END
ORDER BY total_credit_limit DESC, segment_group
LIMIT 5
