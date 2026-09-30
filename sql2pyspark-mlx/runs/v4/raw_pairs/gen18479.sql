WITH cleaned AS (
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
        CONCAT_WS('-', product_name, brand) AS product_name_brand_key,
        SORT_ARRAY(tags) AS sorted_tags
    FROM products
    WHERE supplier_id IS NOT NULL
)
-- Ranking and gap features
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date, id) AS event_seq,
    FIRST_VALUE(price, TRUE) OVER (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_price,
    ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price DESC, id) AS seq_no,
    CORR(stock_qty, price) OVER (PARTITION BY category, brand) AS corr_stock_qty_price
FROM cleaned;
