WITH Base AS (
    SELECT *
    FROM inventory
    WHERE store_id = 1 AND product_id IN (1, 3, 5, 7) AND product_id IN (2, 8)
),
Agg AS (
    SELECT
        product_id,
        CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort,
        AVG(quantity) AS avg_quantity,
        ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM Base
    GROUP BY product_id, CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT
    product_id,
    last_restock_cohort,
    avg_quantity,
    stddev_quantity
FROM Agg
ORDER BY last_restock_cohort DESC
