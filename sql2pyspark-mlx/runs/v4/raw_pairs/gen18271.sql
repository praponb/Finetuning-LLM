WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', category, brand) AS category_brand_key,
        CASE WHEN brand = 'Umbrella' THEN 1 ELSE 0 END AS is_umbrella
    FROM products
    WHERE brand IS NOT NULL
      AND supplier_id IS NULL
)
-- Latest matching inventory per row (LATERAL ... LIMIT 1)
SELECT s.*, x.inventory_quantity
FROM base s
LEFT JOIN LATERAL (
        SELECT c.quantity AS inventory_quantity
        FROM inventory c
        WHERE c.product_id = s.id
        ORDER BY c.quantity DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE
