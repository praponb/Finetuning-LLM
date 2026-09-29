SELECT
    last_restock_quarter,
    quantity_bucket,
    max_quantity,
    cnt
FROM (
    SELECT
        QUARTER(last_restock) AS last_restock_quarter,
        CASE WHEN quantity >= 194 THEN 'high' WHEN quantity >= 31 THEN 'mid' ELSE 'low' END AS quantity_bucket,
        MAX(quantity) AS max_quantity,
        COUNT(*) AS cnt
    FROM inventory
    GROUP BY QUARTER(last_restock), CASE WHEN quantity >= 194 THEN 'high' WHEN quantity >= 31 THEN 'mid' ELSE 'low' END
) AS x
WHERE cnt >= 5
