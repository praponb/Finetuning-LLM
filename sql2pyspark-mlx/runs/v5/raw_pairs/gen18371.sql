WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units, id) AS seq_no,
        LEAD(revenue, 1) OVER (ORDER BY revenue, id) AS next_revenue
    FROM store_sales
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY next_revenue DESC) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.*,
        b.opened_date,
        b.city
    FROM ranked e
    LEFT JOIN stores b
        ON e.store_id = b.id
),
with_dims AS (
    -- Attach products attributes
    SELECT a.*, p.category
    FROM enriched a
    LEFT JOIN products p
        ON a.product_id = p.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        product_id,
        sale_date,
        revenue,
        next_revenue,
        row_num,
        opened_date,
        city
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY opened_date, id) = 1
),
with_dims_store_sales AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.revenue,
        e.next_revenue,
        e.row_num,
        e.opened_date,
        e.city,
        p.brand
    FROM deduped e
    LEFT JOIN products p
        ON e.product_id = p.id
)
SELECT
    id,
    product_id,
    revenue,
    next_revenue,
    row_num,
    opened_date,
    city
FROM with_dims_store_sales
ORDER BY revenue DESC NULLS LAST;
