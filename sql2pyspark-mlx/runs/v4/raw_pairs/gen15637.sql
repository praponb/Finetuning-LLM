WITH joined AS (
    -- Attach products attributes
    SELECT
        s.*,
        p.launch_date,
        p.brand
    FROM order_items s
    INNER JOIN products p
        ON s.product_id = p.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    COUNT(*) FILTER (WHERE brand = 'Globex') AS globex_rows,
    COUNT(*) FILTER (WHERE brand = 'Globex') AS globex_rows_2,
    BIT_AND(quantity) AS quantity_bit_and
FROM joined
GROUP BY 1;
