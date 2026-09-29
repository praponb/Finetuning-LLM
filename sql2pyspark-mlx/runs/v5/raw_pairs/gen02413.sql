WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_label,
        CASE WHEN quantity >= 207 THEN 'High' WHEN quantity >= 70 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM inventory
    WHERE last_restock > DATE '2023-05-18'
)
-- QUALIFY-style window filter
SELECT id, last_restock_label
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC NULLS FIRST, id) AS latest_rank
    FROM valid
) ranked
WHERE latest_rank = 1;
