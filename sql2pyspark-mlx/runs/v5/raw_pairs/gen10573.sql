WITH kept_keys AS (
    -- Set difference on keys
    SELECT id, supplier_id
    FROM products
    EXCEPT
    SELECT id, supplier_id
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'eco')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM products s
    INNER JOIN kept_keys k ON s.id = k.id AND s.supplier_id = k.supplier_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        category,
        price,
        cost,
        launch_date,
        supplier_id,
        ROUND(cost / SUM(cost) OVER (PARTITION BY brand), 4) AS cost_share,
        CORR(cost, stock_qty) OVER (PARTITION BY brand, category) AS corr_cost_stock_qty,
        DENSE_RANK() OVER (ORDER BY cost DESC NULLS FIRST) AS cost_dense_rank,
        STDDEV_SAMP(price) OVER (PARTITION BY brand) AS brand_stddev_samp_price,
        MAX(stock_qty) OVER (PARTITION BY supplier_id) AS supplier_id_max_stock_qty
    FROM kept_rows
    WHERE stock_qty <= 373
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER w_part AS cost_share_cume_dist,
        DENSE_RANK() OVER (ORDER BY cost_dense_rank DESC) AS cost_dense_rank_dense_rank,
        STDDEV_SAMP(brand_stddev_samp_price) OVER w_grp AS category_stddev_samp_brand_stddev_samp_price,
        DENSE_RANK() OVER (ORDER BY cost_dense_rank) AS cost_dense_rank_dense_rank_2
    FROM with_windows
    WINDOW w_part AS (ORDER BY cost_share NULLS FIRST), w_grp AS (PARTITION BY category, supplier_id)
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT supplier_id, id
    FROM metrics
    INTERSECT
    SELECT supplier_id, id
    FROM products
    WHERE cost < 210
),
kept_rows_products AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.supplier_id = k.supplier_id AND s.id = k.id
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        s.*,
        d.country,
        d.rating
    FROM kept_rows_products s
    INNER JOIN suppliers d
        ON s.supplier_id = d.id
)
SELECT
    id,
    category,
    price,
    cost,
    supplier_id,
    brand_stddev_samp_price,
    cost_share_cume_dist,
    category_stddev_samp_brand_stddev_samp_price,
    cost_dense_rank_dense_rank_2
FROM joined;
