WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(store_name, 'Unknown') store_name_filled,
        COALESCE(region, 'N/A') region_filled
    FROM stores
    WHERE opened_date >= DATE '2023-09-01'
      AND UPPER(TRIM(store_name)) = 'DOWNTOWN EXPRESS'
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.region,
        s.store_name_filled,
        s.region_filled,
        latest.last_restock,
        latest.quantity
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.units IS NOT NULL
        )
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        city,
        region,
        store_name_filled,
        last_restock,
        quantity
    FROM not_flagged
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY quantity NULLS LAST, id) = 1
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.store_sale_sale_date,
        top1.store_sale_revenue
    FROM latest s
    JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        region,
        store_sale_revenue
    FROM with_latest
) src
PIVOT (
    SUM(store_sale_revenue) AS amount,
    COUNT(1) AS cnt
    FOR region IN ('West', 'South')
)
