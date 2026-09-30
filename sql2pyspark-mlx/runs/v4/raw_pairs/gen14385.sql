SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity NULLS LAST, id) AS latest_rank
    FROM order_items
) t
WHERE latest_rank = 1
