WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM products
    QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id, category ORDER BY cost DESC, id) = 1
)
SELECT
    id,
    product_name,
    category,
    brand,
    price,
    launch_date,
    tags
FROM deduped
ORDER BY brand DESC NULLS FIRST;
