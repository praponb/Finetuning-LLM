WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(sqft) AS sqft_whole,
        COALESCE(region, 'NONE') AS resolved_region
    FROM stores
    WHERE region IS NOT NULL
)
-- Anti-join via a correlated subquery
SELECT s.*
FROM cleaned s
WHERE NOT EXISTS (
        SELECT 1
        FROM store_sales c
        WHERE c.store_id = s.id
          AND c.sale_date < s.opened_date
    );
