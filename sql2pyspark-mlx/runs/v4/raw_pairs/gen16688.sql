WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        ADD_MONTHS(opened_date, 12) AS opened_date_plus_months
    FROM stores
    WHERE store_name LIKE 'Air%'
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.sale_date,
        x.units
    FROM filtered s
    JOIN LATERAL (
            SELECT c.sale_date, c.units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.store_sale_sale_date,
        x.store_sale_revenue
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(sqft * 1.15, 2) AS sqft_scaled
    FROM latest_match
    WHERE store_sale_revenue >= 3100
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        region,
        city,
        sqft
    FROM valid
) src
PIVOT (
    MAX(sqft) AS amount,
    COUNT(1) AS item_count
    FOR city IN ('Singapore' AS singapore, 'New York' AS new_york, 'Chiang Mai' AS chiang_mai)
);
