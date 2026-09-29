WITH base AS (
    -- Keep recent, valid rows
    SELECT *, quantity + unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND unit_price < 380
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY line_total DESC, id) = 1
),
joined AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.price,
        p.launch_date,
        p.category
    FROM ranked src
    LEFT JOIN products p
        ON src.product_id = p.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_total,
        price,
        launch_date,
        category,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY category), 4) AS quantity_share,
        LEAD(unit_price, 1) OVER (PARTITION BY category ORDER BY quantity, id) AS next_unit_price,
        RANK() OVER (PARTITION BY product_id, category ORDER BY unit_price) AS unit_price_rank,
        RANK() OVER (ORDER BY price DESC NULLS LAST) AS price_rank
    FROM joined
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_total,
        price,
        launch_date,
        category,
        quantity_share,
        next_unit_price,
        unit_price_rank,
        price_rank,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY launch_date DESC, id) AS event_seq,
        LAST_VALUE(quantity, TRUE) OVER (PARTITION BY product_id, category ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity
    FROM scored
)
-- Running and moving aggregates
SELECT
    *,
    DENSE_RANK() OVER (ORDER BY last_known_quantity DESC) AS last_known_quantity_dense_rank,
    ROUND(price_rank / SUM(price_rank) OVER (PARTITION BY order_id, category), 4) AS price_rank_share,
    LEAD(price, 1) OVER (ORDER BY launch_date, id) AS next_price
FROM enriched;
