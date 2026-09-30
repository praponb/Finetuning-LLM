WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        revenue + units AS revenue_per_unit,
        ROUND(revenue * 100, 2) AS adjusted_revenue
    FROM store_sales
    WHERE sale_date > DATE '2023-08-13'
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        src.*,
        b.city,
        b.opened_date,
        b.sqft
    FROM cleaned src
    JOIN stores b
        ON src.store_id = b.id
),
enriched AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        p.cost
    FROM with_dims a
    LEFT JOIN products p
        ON a.product_id = p.id
    WHERE ARRAY_CONTAINS(p.tags, 'sale')
),
enriched_store_sales AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(cost, 2) OVER w_grp AS next_cost,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM enriched
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY cost, id)
)
SELECT
    id,
    product_id,
    cost
FROM enriched_store_sales
ORDER BY id, cost
