WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        product_id,
        sale_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY revenue DESC, id) AS rank_in_group
        FROM store_sales
    ) t
    WHERE rank_in_group = 1
)
SELECT id, sale_date
FROM ranked
ORDER BY sale_date NULLS FIRST, id
LIMIT 20;
