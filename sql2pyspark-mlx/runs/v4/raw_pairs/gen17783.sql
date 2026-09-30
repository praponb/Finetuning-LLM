WITH ranked AS (
    SELECT *
    FROM products
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand, category ORDER BY launch_date ASC NULLS LAST, id ASC) = 1
),
enriched AS (
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
        NTILE(10) OVER w_recent cost_bucket,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY launch_date DESC, id ASC) event_seq
    FROM ranked
    WINDOW w_recent AS (ORDER BY cost ASC, id ASC)
),
filtered AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        tags,
        cost_bucket,
        ROUND(price, 2) price_rounded
    FROM enriched
    WHERE launch_date > TO_DATE('2023-06-01')
)
SELECT *
FROM (
    SELECT
        id,
        category,
        price,
        price_rounded
    FROM filtered
) src
UNPIVOT (
    metric_value FOR kpi IN (price, price_rounded)
);
