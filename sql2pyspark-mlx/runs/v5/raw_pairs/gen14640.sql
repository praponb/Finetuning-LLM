WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY order_id, product_id), 4) AS quantity_share,
        SUM(unit_price) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price
    FROM order_items
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY running_unit_price DESC NULLS LAST, id) AS rn
        FROM scored
    ) x
    WHERE rn = 1
),
joined AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.cost,
        p.product_name
    FROM latest a
    LEFT JOIN products p
        ON a.product_id <=> p.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY quantity NULLS LAST) AS rank_in_group
    FROM joined
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    order_id,
    product_id,
    unit_price,
    quantity_share,
    running_unit_price,
    rank_in_group
FROM deduped
ORDER BY product_id DESC NULLS LAST
