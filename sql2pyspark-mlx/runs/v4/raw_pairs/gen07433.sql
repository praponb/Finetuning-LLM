WITH ranked AS (
    SELECT *
    FROM order_items
    QUALIFY RANK() OVER (PARTITION BY product_id, order_id ORDER BY quantity) = 1
),
ranked_order_items AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity NULLS LAST, id) AS rank_in_group
        FROM ranked
    ) x
    WHERE rank_in_group = 1
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY unit_price DESC, id) AS rn
    FROM ranked_order_items
    QUALIFY rn = 1
)
SELECT
    CASE WHEN quantity >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    order_id,
    BIT_AND(quantity) AS quantity_bit_and,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT order_id) AS distinct_order_ids
FROM latest
GROUP BY CASE WHEN quantity >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, order_id
ORDER BY quantity_bit_and NULLS FIRST
