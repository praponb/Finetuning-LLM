WITH filtered AS (
    SELECT *, CEIL(revenue) AS revenue_whole
    FROM store_sales
    WHERE units > 13
)
SELECT
    d.region,
    COUNT(*) AS rows_total,
    AVG(s.revenue) AS avg_revenue
FROM filtered s
INNER JOIN stores d ON s.store_id = d.id
GROUP BY d.region;
