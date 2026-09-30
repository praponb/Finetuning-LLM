WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('/', city, store_name) AS city_store_name_key,
        DATE_SUB(opened_date, 37) AS opened_date_minus_37d
    FROM stores
    WHERE store_name LIKE '%River%'
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.shipping_fee,
        top1.order_date
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.shipping_fee, c.order_date
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY order_date DESC, id) AS rank_in_group
        FROM with_last
    ) ranked
    WHERE rank_in_group = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date_minus_37d DESC NULLS FIRST, id) AS latest_rank
    FROM latest
    QUALIFY latest_rank = 1
),
with_last_stores AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.revenue,
        x.sale_date
    FROM ranked s
    JOIN LATERAL (
            SELECT c.revenue, c.sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last_stores s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.store_id IN (2, 3, 6)
        )
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        sqft,
        city_store_name_key,
        opened_date_minus_37d,
        order_date,
        latest_rank,
        revenue,
        latest_rank / NULLIF(shipping_fee, 0) AS latest_rank_per_shipping_fee
    FROM not_flagged
    WHERE region IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        city,
        revenue,
        latest_rank_per_shipping_fee
    FROM base
) src
UNPIVOT (
    reading FOR metric_name IN (revenue, latest_rank_per_shipping_fee AS LATEST_RANK_PER_SHIPPING_FEE)
)
