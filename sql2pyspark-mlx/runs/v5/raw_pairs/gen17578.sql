SELECT
    carrier,
    SUM(cost) AS total_cost,
    COUNT(*) AS cnt,
    ROUND(SUM(cost) * 100.0 / SUM(SUM(cost)) OVER (), 2) AS pct_of_total
FROM shipments
WHERE cost < 40
GROUP BY carrier
ORDER BY cnt DESC, total_cost
