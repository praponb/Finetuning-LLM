WITH valid AS (
    SELECT
        *,
        FLOOR(sqft) AS sqft_whole,
        COALESCE(region, 'DIRECT') AS region_filled
    FROM stores
    WHERE opened_date >= DATE '2023-04-26'
      AND region IS NOT NULL
),
deduped AS (
    SELECT *, RANK() OVER (PARTITION BY city, region ORDER BY sqft_whole NULLS LAST) AS row_num
    FROM valid
    QUALIFY row_num = 1
),
with_last AS (
    SELECT s.*, x.sale_date
    FROM deduped s
    JOIN LATERAL (
            SELECT c.sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date <= s.opened_date
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock > '2025-03-01'
        )
)
SELECT
    id,
    city,
    region,
    opened_date,
    sqft,
    sqft_whole,
    row_num,
    sale_date
FROM eligible;
