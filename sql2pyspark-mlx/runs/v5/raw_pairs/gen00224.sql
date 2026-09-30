WITH Base AS (
    SELECT *
    FROM inventory
    WHERE last_restock <= '2023-05-18' AND product_id IN (2, 5, 6) AND last_restock > '2024-04-23'
),
Agg AS (
    SELECT MONTH(last_restock) last_restock_month, SUM(quantity) total_quantity
    FROM Base
    GROUP BY MONTH(last_restock)
)
SELECT last_restock_month, total_quantity
FROM Agg
WHERE total_quantity >= 900
ORDER BY total_quantity DESC, last_restock_month DESC
