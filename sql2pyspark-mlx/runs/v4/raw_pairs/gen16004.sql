WITH StoreTotals AS (
    SELECT store_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) channel_values, ROUND((100 * SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_cancelled
    FROM orders
    GROUP BY store_id
)
SELECT
    s.id,
    s.store_name,
    s.sqft,
    S.channel_values,
    S.pct_cancelled
FROM stores s
INNER JOIN StoreTotals S ON s.id = S.store_id
ORDER BY 3 DESC
