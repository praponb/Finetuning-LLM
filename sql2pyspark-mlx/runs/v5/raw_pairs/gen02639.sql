WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price - quantity AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.order_id,
        e.product_id,
        e.unit_price,
        e.line_total,
        p.brand,
        p.cost,
        p.launch_date
    FROM cleaned e
    LEFT JOIN products p
        ON e.product_id = p.id
),
enriched_order_items AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        unit_price,
        launch_date,
        SUM(cost) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_cost,
        COUNT(cost) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_cost,
        MAX(cost) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_cost,
        SUM(unit_price) OVER (PARTITION BY product_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price
    FROM enriched
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.running_sum_cost,
        src.running_count_cost,
        src.cumulative_unit_price,
        p.amount,
        p.order_date
    FROM enriched_order_items src
    INNER JOIN orders p
        ON src.order_id = p.id
    WHERE p.amount < 430
),
enriched_order_items_2 AS (
    -- Attach orders attributes
    SELECT
        e.*,
        o.shipping_fee,
        o.channel,
        o.amount AS order_amount
    FROM joined e
    LEFT JOIN orders o
        ON e.order_id = o.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cumulative_unit_price NULLS FIRST, id) AS dedup_rank
        FROM enriched_order_items_2
    ) t
    WHERE dedup_rank = 1
)
SELECT
    id,
    running_count_cost,
    cumulative_unit_price,
    amount,
    order_date,
    shipping_fee
FROM ranked
ORDER BY id NULLS FIRST;
