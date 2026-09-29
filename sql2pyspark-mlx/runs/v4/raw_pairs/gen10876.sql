WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN price >= 950 THEN 'High' WHEN price >= 530 THEN 'Medium' ELSE 'Low' END price_band,
        UPPER(category) category_upper
    FROM products
    WHERE supplier_id IS NOT NULL
      AND stock_qty >= 145
)
-- Deduplicate with ROW_NUMBER
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date DESC, id ASC) AS row_num
    FROM filtered
) x
WHERE row_num = 1;
