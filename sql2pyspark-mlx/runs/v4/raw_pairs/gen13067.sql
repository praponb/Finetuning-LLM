WITH filtered_inventory AS (
    SELECT *, DATE_ADD(last_restock, 25) AS last_restock_plus_25d
    FROM inventory
    WHERE quantity BETWEEN 110 AND 161
),
agg AS (
    SELECT DATE_TRUNC('MONTH', last_restock) AS last_restock_month, MIN(last_restock) AS first_last_restock, MIN(last_restock) AS min_last_restock
    FROM filtered_inventory
    GROUP BY DATE_TRUNC('MONTH', last_restock)
)
SELECT last_restock_month, first_last_restock, min_last_restock
FROM agg;
