WITH filtered_inventory AS (
    SELECT *
    FROM inventory
    WHERE store_id IN (2, 3, 4)
),
last_restock_cohort_summary AS (
    SELECT CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort, CASE WHEN quantity >= 233 THEN 'high' WHEN quantity >= 58 THEN 'mid' ELSE 'low' END AS quantity_bucket, MIN(quantity) AS min_quantity
    FROM filtered_inventory
    GROUP BY CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END, CASE WHEN quantity >= 233 THEN 'high' WHEN quantity >= 58 THEN 'mid' ELSE 'low' END
)
SELECT last_restock_cohort, quantity_bucket, min_quantity
FROM last_restock_cohort_summary;
