SELECT
    is_flagged,
    amount_bucket,
    total_amount,
    pct_starbucks
FROM (
    SELECT
        is_flagged,
        CASE WHEN amount >= 4100 THEN 'high' WHEN amount >= 1600 THEN 'mid' ELSE 'low' END AS amount_bucket,
        SUM(amount) AS total_amount,
        ROUND((100 * SUM(CASE WHEN merchant = 'Starbucks' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_starbucks
    FROM transactions
    GROUP BY is_flagged, CASE WHEN amount >= 4100 THEN 'high' WHEN amount >= 1600 THEN 'mid' ELSE 'low' END
) AS x
WHERE total_amount > 7000;
