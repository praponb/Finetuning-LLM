WITH store_summary AS (
    SELECT store_id, ROUND((100 * SUM(CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_shipped, ROUND((100 * SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cancelled
    FROM orders
    GROUP BY store_id
)
SELECT
    stores.opened_date,
    stores.sqft,
    store_summary.pct_shipped,
    store_summary.pct_cancelled
FROM stores
LEFT OUTER JOIN store_summary ON stores.id = store_summary.store_id
WHERE store_summary.pct_cancelled > 33;
