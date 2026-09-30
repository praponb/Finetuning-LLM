WITH filtered AS (
    SELECT
        *,
        CASE WHEN city IN ('Bangkok', 'Chiang Mai') THEN 'Priority' ELSE 'Other' END AS city_group,
        COALESCE(region, 'N/A') AS region_clean
    FROM stores
    WHERE opened_date >= TO_DATE('2023-04-11')
)
SELECT
    s.id,
    s.store_name,
    x.inventory_quantity,
    x.inventory_last_restock
FROM filtered s
JOIN LATERAL (
        SELECT c.quantity AS inventory_quantity, c.last_restock AS inventory_last_restock
        FROM inventory c
        WHERE c.store_id = s.id
        ORDER BY c.quantity DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE;
