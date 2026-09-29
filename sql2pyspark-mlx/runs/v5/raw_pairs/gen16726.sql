WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        supplier_id,
        MIN(price) OVER (PARTITION BY brand, category) AS brand_min_price,
        ROW_NUMBER() OVER w_hist AS seq_no,
        ROUND(price / SUM(price) OVER (PARTITION BY brand), 4) AS price_share,
        ROW_NUMBER() OVER w_hist_2 AS row_seq
    FROM products
    WINDOW w_hist AS (ORDER BY price ASC, id ASC), w_hist_2 AS (ORDER BY stock_qty DESC, id ASC)
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.supplier_id,
    s.brand_min_price,
    s.seq_no,
    s.price_share,
    CASE WHEN category RLIKE '^(Electronics|Home)$' THEN 'PRIORITY' WHEN category RLIKE '^(Sports|Books)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS category_zone,
    (SELECT MAX(x.price) FROM products x WHERE x.supplier_id = s.supplier_id) AS max_product_price
FROM with_windows s
ORDER BY supplier_id DESC
