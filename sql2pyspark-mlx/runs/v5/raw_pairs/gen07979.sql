WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(revenue) OVER (PARTITION BY product_id, store_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_revenue,
        SUM(revenue) OVER (PARTITION BY product_id, store_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_revenue,
        MAX(revenue) OVER (PARTITION BY product_id, store_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_revenue,
        LEAD(revenue, 1) OVER (ORDER BY units, id) AS next_revenue
    FROM store_sales
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        a.sale_date,
        a.units,
        a.revenue,
        a.next_revenue,
        d.price
    FROM with_windows a
    INNER JOIN products d
        ON a.product_id = d.id
),
joined AS (
    -- Attach products attributes
    SELECT s.*, p.cost
    FROM with_dims s
    LEFT JOIN products p
        ON s.product_id = p.id
    WHERE p.stock_qty <> 232
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC NULLS FIRST, id) AS latest_rank
    FROM joined
    QUALIFY latest_rank = 1
),
joined_store_sales AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p.sqft,
        p.store_name
    FROM deduped s
    INNER JOIN stores p
        ON s.store_id = p.id
),
deduped_store_sales AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY cost DESC NULLS FIRST, id) AS rn
        FROM joined_store_sales
    ) x
    WHERE rn = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC, id) AS row_num
        FROM deduped_store_sales
    ) t
    WHERE row_num = 1
)
SELECT
    s.id,
    s.product_id,
    s.sale_date,
    s.revenue,
    s.next_revenue,
    s.sqft,
    s.rn,
    s.row_num,
    (SELECT SUM(x.quantity) FROM order_items x WHERE x.product_id = s.product_id) AS sum_order_item_quantity
FROM latest s;
