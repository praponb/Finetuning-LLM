WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY units DESC NULLS FIRST, id) AS rank_in_group
    FROM store_sales
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    sale_date,
    revenue,
    rank_in_group
FROM latest
ORDER BY id DESC NULLS FIRST
