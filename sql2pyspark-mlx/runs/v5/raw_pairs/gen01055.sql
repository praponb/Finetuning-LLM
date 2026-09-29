WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(revenue, 0) revenue_filled,
        LAST_DAY(sale_date) sale_date_month_end
    FROM store_sales
    WHERE sale_date >= DATE '2023-04-25'
),
joined AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.brand,
        p.cost,
        p.category
    FROM filtered a
    INNER JOIN products p
        ON a.product_id = p.id
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, store_id
    FROM joined
    INTERSECT
    SELECT id, store_id
    FROM store_sales
    WHERE units != 13
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN kept_keys k ON s.id = k.id AND s.store_id = k.store_id
),
joined_store_sales AS (
    -- Attach products attributes
    SELECT e.*, b.cost AS product_cost
    FROM kept_rows e
    LEFT JOIN products b
        ON e.product_id = b.id
)
SELECT
    revenue,
    sale_date_month_end,
    cost,
    product_cost
FROM joined_store_sales;
