WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE last_restock >= DATE '2023-08-09'
      AND store_id = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        quantity_filled,
        DATE_TRUNC('YEAR', last_restock) AS last_restock_period,
        CASE WHEN quantity >= 224 THEN 'High' WHEN quantity >= 98 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM filtered
    WHERE store_id IN (2, 4, 5)
),
with_dims AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.quantity_filled,
        s.last_restock_period,
        s.quantity_tier,
        d.price,
        d.cost,
        d.product_name
    FROM base s
    INNER JOIN products d
        ON s.product_id = d.id
),
joined AS (
    -- Attach products attributes
    SELECT e.*, d.cost AS product_cost
    FROM with_dims e
    LEFT JOIN products d
        ON e.product_id = d.id
    WHERE d.category <> 'Books'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(cost) OVER (PARTITION BY product_id) AS product_id_avg_cost,
        RANK() OVER (ORDER BY product_cost DESC NULLS FIRST) AS product_cost_rank,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY price DESC, id) AS seq_no,
        NTILE(4) OVER (ORDER BY quantity_filled DESC, id) AS quantity_filled_bucket,
        FIRST_VALUE(cost, TRUE) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost
    FROM joined
),
metrics_inventory AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(product_cost_rank) OVER w_recent AS running_max_product_cost_rank,
        COUNT(product_cost_rank) OVER w_recent AS running_count_product_cost_rank,
        SUM(product_cost_rank) OVER w_recent AS running_sum_product_cost_rank,
        MIN(product_cost_rank) OVER w_recent AS running_min_product_cost_rank,
        LAG(product_cost_rank, 1) OVER (PARTITION BY product_id ORDER BY seq_no, id) AS prev_product_cost_rank
    FROM metrics
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY product_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        quantity_filled,
        last_restock_period,
        price,
        cost,
        product_name,
        product_cost,
        product_id_avg_cost,
        product_cost_rank,
        seq_no,
        quantity_filled_bucket,
        first_known_cost,
        running_max_product_cost_rank,
        running_min_product_cost_rank,
        STDDEV_SAMP(seq_no) OVER (PARTITION BY product_id) AS product_id_stddev_samp_seq_no,
        ROUND(product_cost / SUM(product_cost) OVER (PARTITION BY product_id), 4) AS product_cost_share
    FROM metrics_inventory
)
-- Join a dimension and aggregate
SELECT
    p.brand,
    p.category,
    COUNT(*) AS num_rows,
    SUM(e.price) AS total_price
FROM with_windows e
LEFT JOIN products p ON e.product_id = p.id
GROUP BY GROUPING SETS ((p.brand, p.category), (p.brand), ())
