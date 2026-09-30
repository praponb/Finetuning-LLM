WITH recent_products AS (
    SELECT *
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'sale') AND supplier_id IN (27, 30)
),
agg AS (
    SELECT CASE WHEN brand IN ('Umbrella', 'Stark') THEN 'group_a' ELSE 'other' END AS brand_group, ROUND(STDDEV(price), 2) AS stddev_price, MAX(cost) - MIN(cost) AS cost_range
    FROM recent_products
    GROUP BY CASE WHEN brand IN ('Umbrella', 'Stark') THEN 'group_a' ELSE 'other' END
)
SELECT *
FROM agg
ORDER BY stddev_price DESC;
