WITH enriched AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.last_restock,
        p.price,
        p.launch_date,
        p.cost
    FROM inventory src
    INNER JOIN products p
        ON src.product_id = p.id
    WHERE ARRAY_CONTAINS(p.tags, 'clearance')
),
kept_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM enriched
    EXCEPT
    SELECT product_id, id
    FROM inventory
    WHERE product_id = 2
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.product_id = k.product_id AND s.id = k.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(price) OVER (PARTITION BY product_id) AS product_id_sum_price,
        MIN(cost) OVER (PARTITION BY product_id) AS product_id_min_cost,
        DENSE_RANK() OVER (ORDER BY cost DESC) AS cost_dense_rank
    FROM reconciled
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LAST_DAY(launch_date) AS launch_date_month_end
    FROM metrics
    WHERE launch_date >= '2023-09-01'
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        a.product_id_sum_price,
        a.cost_dense_rank,
        a.launch_date_month_end,
        d.cost AS product_cost
    FROM cleaned a
    LEFT JOIN products d
        ON a.product_id = d.id
)
-- Ranking and gap features
SELECT
    id,
    cost_dense_rank,
    launch_date_month_end,
    product_cost,
    ROUND(product_cost / SUM(product_cost) OVER (PARTITION BY product_id), 4) AS product_cost_share
FROM with_dims
