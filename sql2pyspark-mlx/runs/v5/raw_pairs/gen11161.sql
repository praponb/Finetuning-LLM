WITH deduped AS (
    SELECT
        id,
        supplier_name,
        country
    FROM suppliers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) = 1
)
SELECT id, supplier_name
FROM deduped
