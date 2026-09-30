WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        unit_price,
        GREATEST(unit_price, quantity) AS greatest_unit_price_quantity
    FROM order_items
    WHERE unit_price > 150
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        d.price,
        d.cost
    FROM cleaned a
    LEFT JOIN products d
        ON a.product_id = d.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(cost) OVER w_grp AS running_min_cost,
        SUM(cost) OVER w_grp AS running_sum_cost,
        COUNT(cost) OVER w_grp AS running_count_cost,
        MAX(cost) OVER w_grp AS running_max_cost,
        NTILE(3) OVER w_hist AS cost_bucket
    FROM with_dims
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY cost DESC, id)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        product_id,
        price,
        cost,
        running_sum_cost,
        running_max_cost,
        cost_bucket
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY cost_bucket NULLS FIRST, id) AS latest_rank
        FROM with_windows
    ) ranked
    WHERE latest_rank = 1
),
with_dims_order_items AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.price,
        src.cost,
        src.running_sum_cost,
        src.cost_bucket,
        p.stock_qty
    FROM ranked src
    LEFT JOIN products p
        ON src.product_id = p.id
)
SELECT
    id,
    price,
    stock_qty
FROM with_dims_order_items
ORDER BY id DESC NULLS FIRST
