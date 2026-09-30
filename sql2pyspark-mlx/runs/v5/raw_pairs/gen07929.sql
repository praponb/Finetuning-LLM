WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date DESC, id ASC) AS row_num
    FROM stores
    QUALIFY row_num = 1
)
SELECT
    id,
    city,
    region,
    opened_date,
    sqft
FROM deduped
ORDER BY sqft DESC;
