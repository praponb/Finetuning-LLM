WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(revenue) AS revenue_whole,
        CEIL(revenue) AS revenue_whole_2
    FROM store_sales
    WHERE sale_date >= DATE '2023-11-15'
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.*,
        d.brand,
        d.stock_qty,
        d.price
    FROM cleaned a
    LEFT JOIN products d
        ON a.product_id = d.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_whole,
        revenue_whole_2,
        brand,
        stock_qty,
        price,
        AVG(revenue_whole) OVER (ORDER BY revenue, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS revenue_whole_avg_5,
        LAG(revenue, 2) OVER (PARTITION BY store_id ORDER BY sale_date, id) AS prev_revenue,
        ROW_NUMBER() OVER w_recent AS event_seq,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY stock_qty DESC) AS stock_qty_percent_rank,
        LAG(revenue_whole, 2) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS prev_revenue_whole
    FROM with_dims
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY revenue, id)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue_whole,
        revenue_whole_2,
        brand,
        price,
        revenue_whole_avg_5,
        prev_revenue,
        event_seq,
        stock_qty_percent_rank,
        prev_revenue_whole,
        CASE WHEN brand IN ('Stark', 'Initech') THEN 'Group A' ELSE 'Other' END AS brand_group,
        COALESCE(brand, 'NONE') AS brand_clean
    FROM enriched
    WHERE event_seq >= 4
)
SELECT
    id,
    product_id,
    sale_date,
    units,
    revenue_whole,
    price,
    revenue_whole_avg_5,
    stock_qty_percent_rank,
    prev_revenue_whole
FROM valid;
