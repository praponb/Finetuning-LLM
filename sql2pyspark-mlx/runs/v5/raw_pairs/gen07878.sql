WITH enriched AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY quantity DESC) AS quantity_cume_dist,
        NTILE(5) OVER (ORDER BY unit_price, id) AS unit_price_bucket,
        ROW_NUMBER() OVER (ORDER BY quantity DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY unit_price DESC, id) AS row_seq_2,
        RANK() OVER w_recent AS unit_price_rank
    FROM order_items
    WINDOW w_recent AS (PARTITION BY order_id, product_id ORDER BY unit_price DESC NULLS FIRST)
),
joined AS (
    SELECT s.*, o.amount
    FROM enriched s
    LEFT JOIN orders o
        ON s.order_id = o.id
),
valid AS (
    SELECT
        *,
        unit_price_bucket + row_seq_2 AS unit_price_bucket_plus_row_seq_2,
        unit_price + unit_price_bucket AS unit_price_plus_unit_price_bucket
    FROM joined
    WHERE product_id IS NOT NULL
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY quantity, id) AS dedup_rank
        FROM valid
    ) t
    WHERE dedup_rank = 1
)
SELECT
    id,
    product_id,
    quantity,
    row_seq,
    unit_price_rank,
    amount,
    unit_price_plus_unit_price_bucket,
    dedup_rank,
    ROUND(unit_price_rank / SUM(unit_price_rank) OVER (PARTITION BY product_id), 4) AS unit_price_rank_share,
    NTILE(4) OVER (PARTITION BY order_id, product_id ORDER BY quantity, id) AS quantity_bucket
FROM ranked
WHERE dedup_rank > 1;
