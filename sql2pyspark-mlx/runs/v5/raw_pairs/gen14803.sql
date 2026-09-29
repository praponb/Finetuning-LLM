WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(quantity, unit_price) AS greatest_quantity_unit_price
    FROM order_items
    WHERE order_id IN (11, 18, 23)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price, id) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        rank_in_group,
        LEAD(unit_price) OVER (PARTITION BY product_id ORDER BY unit_price, id) AS next_unit_price,
        LAG(quantity) OVER (PARTITION BY product_id ORDER BY unit_price, id) AS prev_quantity,
        LEAD(quantity) OVER (PARTITION BY product_id ORDER BY unit_price, id) AS next_quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price, id) AS row_seq,
        AVG(quantity) OVER (PARTITION BY order_id ORDER BY rank_in_group, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior
    FROM deduped
),
with_dims AS (
    -- Attach products attributes
    SELECT src.*, d.price
    FROM scored src
    LEFT JOIN products d
        ON src.product_id = d.id
    WHERE d.product_name LIKE '%Lapto%'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        rank_in_group,
        next_unit_price,
        prev_quantity,
        row_seq,
        price,
        LAST_VALUE(row_seq, TRUE) OVER (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_row_seq,
        ROW_NUMBER() OVER (ORDER BY next_unit_price DESC, id) AS event_seq
    FROM with_dims
)
SELECT
    s.id,
    s.product_id,
    s.next_unit_price,
    s.prev_quantity,
    s.row_seq,
    s.price,
    s.last_known_row_seq,
    s.event_seq,
    (SELECT AVG(t2.units) FROM store_sales t2 WHERE t2.product_id = s.product_id) AS avg_store_sale_units
FROM enriched s;
