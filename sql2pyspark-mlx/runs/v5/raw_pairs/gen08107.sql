SELECT
    d.city,
    d.segment,
    COUNT(*) AS num_rows,
    MIN(a.monthly_fee) AS min_monthly_fee,
    GROUPING_ID() AS grouping_level
FROM subscriptions a
JOIN customers d ON a.customer_id = d.id
GROUP BY ROLLUP (d.city, d.segment)
ORDER BY num_rows DESC
