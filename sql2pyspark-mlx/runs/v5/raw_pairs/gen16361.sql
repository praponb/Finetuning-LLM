WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_SUB(last_restock, 29) AS last_restock_minus_29d
    FROM inventory
    WHERE last_restock > TO_DATE('2023-04-28')
      AND quantity <= 98
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(quantity) OVER w_hist AS prev_quantity,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM valid
    WINDOW w_hist AS (PARTITION BY product_id, store_id ORDER BY quantity, id)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        row_seq,
        SUM(row_seq) OVER w_part AS running_row_seq,
        DENSE_RANK() OVER w_grp AS row_seq_dense_rank
    FROM with_windows
    WINDOW w_part AS (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY row_seq DESC NULLS FIRST)
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.last_restock,
        s.row_seq,
        s.running_row_seq,
        p.brand,
        p.launch_date,
        p.stock_qty
    FROM metrics s
    LEFT JOIN products p
        ON s.product_id = p.id
),
enriched_inventory AS (
    -- Attach stores attributes
    SELECT
        e.*,
        d.opened_date,
        d.sqft,
        d.region
    FROM enriched e
    INNER JOIN stores d
        ON e.store_id = d.id
    WHERE d.region NOT IN ('North', 'East')
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY launch_date DESC, id) AS rank_in_group
        FROM enriched_inventory
    ) x
    WHERE rank_in_group = 1
),
with_windows_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(running_row_seq) OVER (PARTITION BY brand ORDER BY launch_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS running_row_seq_avg_3,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY sqft DESC, id) AS sqft_bucket
    FROM latest
)
-- Join a dimension and aggregate
SELECT
    d.brand AS product_brand,
    s.region,
    COUNT(*) AS num_rows,
    ROUND(AVG(s.sqft), 2) AS avg_sqft,
    SUM(s.stock_qty) AS total_stock_qty
FROM with_windows_inventory s
INNER JOIN products d ON s.product_id = d.id
GROUP BY CUBE (d.brand, s.region)
