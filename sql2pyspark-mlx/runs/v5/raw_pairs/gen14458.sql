WITH filtered AS (
    SELECT
        id,
        order_id,
        product_id,
        unit_price + quantity AS line_total,
        CAST(unit_price AS INT) AS unit_price_int
    FROM order_items
    WHERE product_id IS NOT NULL
      AND quantity <> 4
)
SELECT id, product_id
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price_int DESC, id) AS dedup_rank
    FROM filtered
) ranked
WHERE dedup_rank = 1
