WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(quantity) AS quantity_whole
    FROM inventory
    WHERE product_id IN (2, 4, 5, 9)
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.quantity_whole,
        d.cost,
        d.stock_qty,
        d.brand
    FROM valid e
    LEFT JOIN products d
        ON e.product_id = d.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY product_id, store_id ORDER BY cost DESC) AS rn
    FROM with_dims
    QUALIFY rn = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity_whole DESC, id ASC) AS latest_rank
    FROM latest
    QUALIFY latest_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY store_id ORDER BY cost ASC, id ASC) AS cost_bucket,
        LAG(stock_qty, 1) OVER (ORDER BY cost ASC, id ASC) AS prev_stock_qty,
        AVG(cost) OVER (PARTITION BY product_id ORDER BY latest_rank ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS cost_avg_5_prior
    FROM deduped
),
summary AS (
    -- Grouped metrics
    SELECT
        brand,
        store_id,
        COUNT(*) FILTER (WHERE brand = 'Acme') AS acme_rows,
        SIZE(COLLECT_SET(brand)) AS num_brands,
        ROUND(AVG(cost), 2) AS avg_cost,
        COUNT(*) AS num_rows
    FROM scored
    GROUP BY GROUPING SETS ((brand, store_id), (brand))
)
SELECT
    s.brand,
    s.store_id,
    s.acme_rows,
    s.num_brands,
    s.num_rows,
    (SELECT SUM(x.quantity) FROM inventory x WHERE x.store_id = s.store_id) AS sum_inventory_quantity
FROM summary s
ORDER BY sum_inventory_quantity ASC;
