WITH latest AS (
    SELECT id, revenue
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id) AS latest_rank
        FROM store_sales
    ) ranked
    WHERE latest_rank = 1
)
SELECT id
FROM latest
ORDER BY id NULLS FIRST;
