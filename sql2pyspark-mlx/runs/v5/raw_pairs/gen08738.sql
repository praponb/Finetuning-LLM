SELECT
    CASE WHEN rating >= 3.9 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket,
    SUM(rating) AS total_rating,
    COUNT(*) AS cnt,
    ROUND(SUM(rating) * 100.0 / SUM(SUM(rating)) OVER (), 2) AS pct_of_total
FROM suppliers
GROUP BY CASE WHEN rating >= 3.9 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END;
