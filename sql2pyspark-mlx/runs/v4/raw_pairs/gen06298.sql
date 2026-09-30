WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        NTILE(5) OVER (ORDER BY cost, id) AS cost_bucket,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY launch_date, id) AS event_seq,
        CORR(cost, price) OVER w_ordered AS corr_cost_price,
        CORR(cost, stock_qty) OVER w_ordered AS corr_cost_stock_qty,
        RANK() OVER w_grp AS stock_qty_rank
    FROM products
    WINDOW w_ordered AS (PARTITION BY supplier_id), w_grp AS (PARTITION BY category ORDER BY stock_qty DESC NULLS LAST)
)
SELECT
    id,
    product_name,
    category,
    price,
    cost,
    stock_qty,
    launch_date,
    tags,
    supplier_id,
    CASE WHEN product_name RLIKE '^(Backpack XL|Coffee Maker)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS product_name_zone
FROM enriched
ORDER BY supplier_id
