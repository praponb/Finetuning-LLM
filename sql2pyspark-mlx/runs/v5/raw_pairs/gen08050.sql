WITH base AS (
    -- Filter the raw rows and derive helper columns
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
        ARRAY_CONTAINS(tags, 'new') is_new
    FROM products
    WHERE stock_qty >= 390
)
-- Window metrics over several different partitions
SELECT
    *,
    ROW_NUMBER() OVER w_part event_seq,
    RANK() OVER w_grp cost_rank,
    NTILE(4) OVER (PARTITION BY supplier_id ORDER BY price, id) price_bucket,
    FIRST_VALUE(brand) IGNORE NULLS OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_brand,
    ROW_NUMBER() OVER (ORDER BY price, id) row_seq
FROM base
WHERE cost BETWEEN 370 AND 620
WINDOW w_part AS (PARTITION BY supplier_id ORDER BY price DESC, id), w_grp AS (PARTITION BY brand ORDER BY cost DESC NULLS FIRST)
