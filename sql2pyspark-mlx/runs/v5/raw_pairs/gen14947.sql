WITH joined AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        a.quantity,
        p.price,
        p.cost,
        p.product_name
    FROM inventory a
    LEFT JOIN products p
        ON a.product_id = p.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(cost) OVER w_ordered AS prev_cost,
        LEAD(cost) OVER w_ordered AS next_cost,
        LEAD(price) OVER w_ordered AS next_price,
        LAG(price) OVER w_ordered AS prev_price,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        LAST_VALUE(quantity, TRUE) OVER w_ordered_2 AS last_known_quantity
    FROM joined
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY price, id), w_ordered_2 AS (PARTITION BY product_id ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        price,
        cost,
        next_price,
        event_seq,
        last_known_quantity,
        RANK() OVER w_part AS price_rank,
        SUM(price) OVER (PARTITION BY product_id) AS product_id_sum_price
    FROM scored
    WINDOW w_part AS (ORDER BY price DESC NULLS FIRST)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        price,
        next_price,
        event_seq,
        product_id_sum_price,
        CUME_DIST() OVER (ORDER BY product_id_sum_price DESC) AS product_id_sum_price_cume_dist,
        NTILE(4) OVER (ORDER BY price_rank DESC, id) AS price_rank_bucket
    FROM with_windows
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, GREATEST(price_rank_bucket, product_id_sum_price_cume_dist) AS greatest_price_rank_bucket_product_id_sum_price_cume_dist
    FROM enriched
    WHERE next_price IS NOT NULL
)
SELECT
    id,
    price,
    next_price,
    event_seq,
    product_id_sum_price,
    product_id_sum_price_cume_dist,
    price_rank_bucket
FROM filtered
ORDER BY product_id_sum_price DESC, id
LIMIT 5
