WITH RecentInventory AS (
    SELECT *, LAST_DAY(last_restock) AS last_restock_month_end
    FROM inventory
    WHERE last_restock < '2025-09-25'
),
agg AS (
    SELECT YEAR(last_restock) AS last_restock_year, MIN(last_restock_month_end) AS min_last_restock_month_end, SUM(quantity) AS total_quantity
    FROM RecentInventory
    GROUP BY YEAR(last_restock)
)
SELECT last_restock_year, min_last_restock_month_end, total_quantity
FROM agg
ORDER BY last_restock_year DESC;
