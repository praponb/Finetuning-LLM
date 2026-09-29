WITH RecentStoreSales AS (
    SELECT *, FLOOR(units) AS units_whole
    FROM store_sales
    WHERE product_id = 3
),
agg AS (
    SELECT store_id, ROUND(STDDEV(revenue), 2) AS stddev_revenue
    FROM RecentStoreSales
    GROUP BY store_id
)
SELECT *
FROM agg
ORDER BY store_id DESC, stddev_revenue DESC
LIMIT 3
